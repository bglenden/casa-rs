// SPDX-License-Identifier: LGPL-3.0-or-later

use std::process::Command;

use casa_provider_contracts::{builtin_surface_bundle, project_ui_schema};

struct ProviderCase {
    surface: &'static str,
    executable: &'static str,
    prefix_args: &'static [&'static str],
}

#[test]
fn every_image_provider_ui_schema_path_matches_its_canonical_surface() {
    let cases = [
        ProviderCase {
            surface: "imhead",
            executable: env!("CARGO_BIN_EXE_imexplore"),
            prefix_args: &["imhead"],
        },
        ProviderCase {
            surface: "imstat",
            executable: env!("CARGO_BIN_EXE_imexplore"),
            prefix_args: &["imstat"],
        },
        ProviderCase {
            surface: "immoments",
            executable: env!("CARGO_BIN_EXE_immoments"),
            prefix_args: &[],
        },
        ProviderCase {
            surface: "impv",
            executable: env!("CARGO_BIN_EXE_impv"),
            prefix_args: &[],
        },
        ProviderCase {
            surface: "imsubimage",
            executable: env!("CARGO_BIN_EXE_imsubimage"),
            prefix_args: &[],
        },
        ProviderCase {
            surface: "immath",
            executable: env!("CARGO_BIN_EXE_immath"),
            prefix_args: &[],
        },
        ProviderCase {
            surface: "impbcor",
            executable: env!("CARGO_BIN_EXE_impbcor"),
            prefix_args: &[],
        },
        ProviderCase {
            surface: "imregrid",
            executable: env!("CARGO_BIN_EXE_imregrid"),
            prefix_args: &[],
        },
        ProviderCase {
            surface: "feather",
            executable: env!("CARGO_BIN_EXE_feather"),
            prefix_args: &[],
        },
        ProviderCase {
            surface: "exportfits",
            executable: env!("CARGO_BIN_EXE_exportfits"),
            prefix_args: &[],
        },
        ProviderCase {
            surface: "importfits",
            executable: env!("CARGO_BIN_EXE_importfits"),
            prefix_args: &[],
        },
        ProviderCase {
            surface: "imexplore",
            executable: env!("CARGO_BIN_EXE_imexplore"),
            prefix_args: &[],
        },
    ];

    for case in cases {
        let output = Command::new(case.executable)
            .args(case.prefix_args)
            .arg("--ui-schema")
            .output()
            .unwrap_or_else(|error| panic!("run {} --ui-schema: {error}", case.surface));
        assert!(
            output.status.success(),
            "{} --ui-schema failed: {}",
            case.surface,
            String::from_utf8_lossy(&output.stderr)
        );
        let actual: serde_json::Value = serde_json::from_slice(&output.stdout)
            .unwrap_or_else(|error| panic!("parse {} --ui-schema: {error}", case.surface));
        let expected = project_ui_schema(
            &builtin_surface_bundle(case.surface)
                .unwrap_or_else(|error| panic!("load {} surface: {error}", case.surface)),
        );
        assert_eq!(
            actual, expected,
            "{} provider parameter names, types, or defaults diverged",
            case.surface
        );
    }
}
