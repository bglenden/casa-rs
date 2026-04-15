// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::PathBuf;

mod common;

use common::{first_existing_path, run_importvla_parity_case};

struct ParityCase {
    name: &'static str,
    env_var: &'static str,
    candidates: &'static [&'static str],
}

const PARITY_CASES: &[ParityCase] = &[
    ParityCase {
        name: "ag189_rev11_continuum_multiband",
        env_var: "CASA_RS_IMPORTVLA_PARITY_AG189_REV11_CONTINUUM",
        candidates: &[
            "/Users/brianglendenning/Desktop/AG189/observation.46182.7646759/AG189_1_46182.76468_46183.09488.exp",
        ],
    },
    ParityCase {
        name: "ag189_rev11_mixed_line_continuum",
        env_var: "CASA_RS_IMPORTVLA_PARITY_AG189_REV11_MIXED",
        candidates: &[
            "/Users/brianglendenning/Desktop/AG189/observation.46325.2302894/AG189_1_46325.23029_46325.80807.exp",
        ],
    },
    ParityCase {
        name: "ag189_rev12_continuum",
        env_var: "CASA_RS_IMPORTVLA_PARITY_AG189_REV12_CONTINUUM",
        candidates: &[
            "/Users/brianglendenning/Desktop/AG189/observation.46673.4830671/AG189_1_46673.48307_46673.81374.exp",
        ],
    },
    ParityCase {
        name: "as758_rev26_reference_pointing",
        env_var: "CASA_RS_IMPORTVLA_PARITY_AS758_XP1",
        candidates: &[
            "/Volumes/home/casatestdata/unittest/importvla/AS758_C030425.xp1",
            "/Users/brianglendenning/SoftwareProjects/casatestdata/unittest/importvla/AS758_C030425.xp1",
            "/Volumes/home/casatestdata/other/AS758_C030425.xp1",
        ],
    },
    ParityCase {
        name: "as758_rev26_tipping_curve",
        env_var: "CASA_RS_IMPORTVLA_PARITY_AS758_XP5",
        candidates: &[
            "/Volumes/home/casatestdata/unittest/importvla/AS758_C030426.xp5",
            "/Users/brianglendenning/SoftwareProjects/casatestdata/unittest/importvla/AS758_C030426.xp5",
            "/Volumes/home/casatestdata/other/AS758_C030426.xp5",
        ],
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
    first_existing_path(case.candidates)
}
