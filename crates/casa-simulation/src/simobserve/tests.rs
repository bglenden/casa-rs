// SPDX-License-Identifier: LGPL-3.0-or-later
//! The run's checks before it reads the sky model.

use super::*;

fn request(parameters: SimobserveParameters) -> SimobserveRequest {
    let resources = crate::SessionResources::from_controls(&crate::ExecutionControls::default())
        .expect("session resources");
    SimobserveRequest {
        parameters,
        data_roots: Vec::new(),
        options: SimulatorOptions {
            measures: casa_test_support::deterministic_measures_provider(),
            resources,
            now: casa_types::measures::MEpoch::from_mjd(
                60_000.0,
                casa_types::measures::EpochRef::UTC,
            ),
        },
    }
}

fn refusal(parameters: SimobserveParameters) -> String {
    match simobserve(&request(parameters)) {
        Ok(outcome) => panic!("expected a refusal, got {outcome:?}"),
        Err(error) => error.to_string(),
    }
}

#[test]
fn the_defaults_are_casas() {
    let defaults = SimobserveParameters::default();
    assert_eq!(defaults.project, "sim");
    assert_eq!(defaults.compwidth, "\"8GHz\"");
    assert_eq!(defaults.mapsize, ["", ""]);
    assert_eq!(defaults.thermalnoise, "tsys-atm");
    assert!(defaults.overwrite);
}

#[test]
fn a_run_needs_an_antenna_list_and_a_sky() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let project = |name: &str| directory.path().join(name).display().to_string();
    let message = refusal(SimobserveParameters {
        project: project("a"),
        ..SimobserveParameters::default()
    });
    assert_eq!(
        message,
        "The name of antenna list (antennalist/sdantlist) is not specified"
    );
    let message = refusal(SimobserveParameters {
        project: project("b"),
        antennalist: "alma.out01.cfg".to_string(),
        ..SimobserveParameters::default()
    });
    assert_eq!(message, "At least one of skymodel or complist must be set.");
    let message = refusal(SimobserveParameters {
        project: project("c"),
        antennalist: "alma.out01.cfg".to_string(),
        skymodel: "missing.image".to_string(),
        ..SimobserveParameters::default()
    });
    assert_eq!(
        message,
        "No sky input found.  At least one of skymodel or complist must exist."
    );
    let message = refusal(SimobserveParameters {
        project: project("d"),
        obsmode: String::new(),
        antennalist: "alma.out01.cfg".to_string(),
        ..SimobserveParameters::default()
    });
    assert_eq!(
        message,
        "Both antennalist and sdantlist are defined. Define only one of them."
    );
}

#[test]
fn an_existing_measurement_set_needs_overwrite() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let project = directory.path().join("p");
    std::fs::create_dir_all(project.join("p.alma.out01.ms")).expect("an MS directory");
    let message = refusal(SimobserveParameters {
        project: project.display().to_string(),
        antennalist: "alma.out01.cfg".to_string(),
        overwrite: false,
        ..SimobserveParameters::default()
    });
    assert_eq!(
        message,
        format!(
            "{}/p.alma.out01.ms exists but overwrite=F",
            project.display()
        )
    );
}
