// SPDX-License-Identifier: LGPL-3.0-or-later

#![cfg(feature = "cpp-interop-tests")]

use std::path::PathBuf;

mod common;

use common::{run_importvla_parity_case, slow_parity_archive};

struct ParityCase {
    name: &'static str,
    env_var: &'static str,
    shared_relative_path: Option<&'static str>,
    candidates: &'static [&'static str],
}

const PARITY_CASES: &[ParityCase] = &[
    ParityCase {
        name: "ag189_rev11_continuum_multiband",
        env_var: "CASA_RS_IMPORTVLA_PARITY_AG189_REV11_CONTINUUM",
        shared_relative_path: None,
        candidates: &[],
    },
    ParityCase {
        name: "ag189_rev11_mixed_line_continuum",
        env_var: "CASA_RS_IMPORTVLA_PARITY_AG189_REV11_MIXED",
        shared_relative_path: None,
        candidates: &[],
    },
    ParityCase {
        name: "ag189_rev12_continuum",
        env_var: "CASA_RS_IMPORTVLA_PARITY_AG189_REV12_CONTINUUM",
        shared_relative_path: None,
        candidates: &[],
    },
    ParityCase {
        name: "as758_rev26_reference_pointing",
        env_var: "CASA_RS_IMPORTVLA_PARITY_AS758_XP1",
        shared_relative_path: Some("unittest/importvla/AS758_C030425.xp1"),
        candidates: &["other/AS758_C030425.xp1"],
    },
    ParityCase {
        name: "as758_rev26_tipping_curve",
        env_var: "CASA_RS_IMPORTVLA_PARITY_AS758_XP5",
        shared_relative_path: Some("unittest/importvla/AS758_C030426.xp5"),
        candidates: &["other/AS758_C030426.xp5"],
    },
];

#[test]
#[ignore = "real-data parity matrix; run explicitly with --ignored --nocapture"]
fn known_real_archives_match_casa_when_available() {
    let mut ran_cases = 0usize;
    for case in PARITY_CASES {
        let Some(path) = resolve_case_path(case) else {
            eprintln!("skipping {}: no archive found", case.name);
            continue;
        };
        eprintln!("running {} with {}", case.name, path.display());
        run_importvla_parity_case(&path);
        ran_cases += 1;
    }
    if ran_cases == 0 {
        eprintln!("skipping: no configured parity-matrix archives found");
    }
}

fn resolve_case_path(case: &ParityCase) -> Option<PathBuf> {
    if let Some(path) = std::env::var_os(case.env_var).map(PathBuf::from) {
        return path.exists().then_some(path);
    }
    if let Some(relative) = case.shared_relative_path {
        if let Some(path) = slow_parity_archive(relative) {
            return Some(path);
        }
    }
    for relative in case.candidates {
        if let Some(path) = slow_parity_archive(relative) {
            return Some(path);
        }
    }
    None
}
