// SPDX-License-Identifier: LGPL-3.0-or-later

use std::env;
use std::path::Path;

use casa_test_support::{
    CasaTestDataTier, TUTORIAL_DATASETS, casa_tutorial_data_root,
    casatestdata_root_for_tier_with_required_paths, tutorial_dataset, tutorial_dataset_path,
};

fn main() {
    let mut args = env::args().skip(1);
    let mut tier = CasaTestDataTier::DefaultFixture;
    let mut required_paths = Vec::new();
    let mut required_registry_keys = Vec::new();
    let mut list_registry = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--tier" => {
                let Some(value) = args.next() else {
                    usage_and_exit(2);
                };
                tier = parse_tier(&value).unwrap_or_else(|| usage_and_exit(2));
            }
            "--require" => {
                let Some(value) = args.next() else {
                    usage_and_exit(2);
                };
                required_paths.push(value);
            }
            "--require-registry-key" => {
                let Some(value) = args.next() else {
                    usage_and_exit(2);
                };
                required_registry_keys.push(value);
            }
            "--list-registry" => list_registry = true,
            "-h" | "--help" => usage_and_exit(0),
            _ => usage_and_exit(2),
        }
    }

    if list_registry {
        for dataset in TUTORIAL_DATASETS {
            println!(
                "{}\t{}\t{}\t{}",
                dataset.key,
                dataset.tier.as_str(),
                dataset.expected_filename,
                dataset.relative_path
            );
        }
        return;
    }

    let root = casatestdata_root_for_tier_with_required_paths(tier, &required_paths);
    if root.is_none() && required_registry_keys.is_empty() {
        eprintln!(
            "casatestdata preflight failed: no {} root found; set CASA_RS_TESTDATA_ROOT or stage ../casatestdata",
            tier.as_str()
        );
        std::process::exit(1);
    };
    if let Some(root) = &root {
        println!(
            "casatestdata preflight: tier={} root={}",
            tier.as_str(),
            root.display()
        );
    }
    if !required_registry_keys.is_empty() {
        match casa_tutorial_data_root() {
            Some(root) => println!("casatestdata preflight: tutorial root={}", root.display()),
            None => eprintln!(
                "casatestdata preflight: no tutorial root found; set CASA_RS_TUTORIAL_DATA_ROOT"
            ),
        }
    }

    let mut missing = Vec::new();
    for relative in &required_paths {
        let Some(root) = &root else {
            eprintln!(
                "casatestdata preflight failed: no {} root found for required path {relative}",
                tier.as_str()
            );
            std::process::exit(1);
        };
        let path = root.join(relative);
        if !path.exists() {
            missing.push(path);
        }
    }
    for key in &required_registry_keys {
        let Some(dataset) = tutorial_dataset(key) else {
            eprintln!("casatestdata preflight failed: unknown registry key {key}");
            std::process::exit(2);
        };
        let Some(path) = tutorial_dataset_path(key) else {
            let fallback_root = casa_tutorial_data_root()
                .or_else(|| root.clone())
                .unwrap_or_else(|| Path::new("casa-tutorial-data").to_path_buf());
            missing.push(fallback_root.join(dataset.relative_path));
            continue;
        };
        if !Path::new(&path).exists() {
            missing.push(path);
        }
    }

    if !missing.is_empty() {
        eprintln!("casatestdata preflight failed: missing required data:");
        for path in missing {
            eprintln!("  - {}", path.display());
        }
        std::process::exit(1);
    }
}

fn parse_tier(value: &str) -> Option<CasaTestDataTier> {
    match value {
        "default-fixture" => Some(CasaTestDataTier::DefaultFixture),
        "tutorial-parity" => Some(CasaTestDataTier::TutorialParity),
        "slow-parity" => Some(CasaTestDataTier::SlowParity),
        _ => None,
    }
}

fn usage_and_exit(code: i32) -> ! {
    eprintln!(
        "usage: casatestdata-preflight [--tier default-fixture|tutorial-parity|slow-parity] [--require RELPATH] [--require-registry-key KEY] [--list-registry]"
    );
    std::process::exit(code);
}
