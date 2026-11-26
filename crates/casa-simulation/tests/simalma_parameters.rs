// SPDX-License-Identifier: LGPL-3.0-or-later
//! Observable exposure allocation, dry runs and project input protection.
use casa_simulation::{ExecutionControls, SessionResources, simalma::simalma};
use serde_json::json;
#[path = "common/simalma_fixture.rs"]
mod fixture;

#[test]
fn scalar_and_vector_times_are_allocated_and_dryrun_publishes_no_data() {
    let resources = SessionResources::from_controls(&ExecutionControls::default()).unwrap();
    let (scratch, mut request, _) = fixture::fixture(resources);
    request.parameters.insert("dryrun".into(), json!(true));
    let scalar = simalma(&request).unwrap();
    assert!(scalar.warnings[0].contains("1.000000 minutes"));
    assert!(scalar.warnings[1].contains("2.000000 minutes"));
    assert!(scalar.warnings[2].contains("2 TP antennas"));
    assert!(scalar.products.is_empty());
    assert!(!scratch.path().join("combo").exists());
    request
        .parameters
        .insert("totaltime".into(), json!(["20min", "30min"]));
    let vector = simalma(&request).unwrap();
    assert!(vector.warnings[0].contains("20.000000 minutes"));
    assert!(vector.warnings[1].contains("30.000000 minutes"));
    assert!(!scratch.path().join("combo").exists());
}

#[test]
fn invalid_time_vector_and_project_owned_inputs_preserve_existing_project() {
    let resources = SessionResources::from_controls(&ExecutionControls::default()).unwrap();
    let (scratch, mut request, _) = fixture::fixture(resources);
    let project = scratch.path().join("combo");
    std::fs::create_dir(&project).unwrap();
    let canary = project.join("caller-data");
    std::fs::write(&canary, b"preserve").unwrap();
    request
        .parameters
        .insert("totaltime".into(), json!(["1min", "2min", "3min"]));
    let error = simalma(&request).err().unwrap().to_string();
    assert!(error.contains("one per configuration"), "{error}");
    assert_eq!(std::fs::read(&canary).unwrap(), b"preserve");
    request.parameters.insert("totaltime".into(), json!("1min"));
    request.parameters.insert("skymodel".into(), json!(canary));
    let error = simalma(&request).err().unwrap().to_string();
    assert!(error.contains("skymodel would be deleted"), "{error}");
    assert_eq!(std::fs::read(&canary).unwrap(), b"preserve");
    request.parameters.insert(
        "skymodel".into(),
        json!(scratch.path().join("inputs/model.image")),
    );
    let config = project.join("alma.test.cfg");
    std::fs::copy(scratch.path().join("inputs/alma.test.cfg"), &config).unwrap();
    request
        .parameters
        .insert("antennalist".into(), json!([config]));
    let error = simalma(&request).err().unwrap().to_string();
    assert!(error.contains("configuration would be deleted"), "{error}");
    assert_eq!(std::fs::read(&canary).unwrap(), b"preserve");
}

#[test]
fn project_relative_and_template_pointing_inputs_are_preserved() {
    let resources = SessionResources::from_controls(&ExecutionControls::default()).unwrap();
    let (scratch, mut request, _) = fixture::fixture(resources);
    let project = scratch.path().join("combo");
    std::fs::create_dir(&project).unwrap();
    request
        .parameters
        .insert("setpointings".into(), json!(false));
    for (name, input) in [
        ("pointings.txt", "pointings.txt"),
        ("$project.ptg.txt", "combo.alma.test.ptg.txt"),
    ] {
        let input = project.join(input);
        std::fs::write(&input, b"J2000 10h00m00 -30d00m00 10\n").unwrap();
        request.parameters.insert("ptgfile".into(), json!(name));
        let error = simalma(&request).err().unwrap().to_string();
        assert!(
            error.contains("pointing input") && error.contains("would be deleted"),
            "{error}"
        );
        assert_eq!(
            std::fs::read(&input).unwrap(),
            b"J2000 10h00m00 -30d00m00 10\n"
        );
    }
}

#[test]
fn tpnant_overrides_custom_tp_array_and_one_array_uses_final_correlator() {
    let resources = SessionResources::from_controls(&ExecutionControls::default()).unwrap();
    let (scratch, mut request, _) = fixture::fixture(resources);
    let root = scratch.path();
    let source = std::fs::read_to_string(root.join("inputs/alma/simmos/aca.tp.cfg")).unwrap();
    let mut custom = String::new();
    for line in source.lines().filter(|line| line.starts_with('#')) {
        custom.push_str(line);
        custom.push('\n');
    }
    custom.push_str(
        source
            .lines()
            .find(|line| !line.is_empty() && !line.starts_with('#'))
            .unwrap(),
    );
    custom.push('\n');
    let configuration = root.join("aca.customtp.cfg");
    std::fs::write(&configuration, custom).unwrap();
    request.parameters.insert(
        "antennalist".into(),
        json!([root.join("inputs/alma.test.cfg"), configuration]),
    );
    request
        .parameters
        .insert("totaltime".into(), json!(["60s", "60s"]));
    request.parameters.insert("pwv".into(), json!(0.6));
    request.parameters.insert("image".into(), json!(false));
    // CASA task_simalma.py:503–512 overrides a listed TP file when tpnant>0.
    // Its :903–907 chooses the final correlator if len>nconfigs. Thus the
    // default three-entry list and this two-entry list have identical TP
    // settings for one interferometer. Fixed native streams must match.
    simalma(&request).unwrap();
    let paths = [
        "combo.aca.tp.noisy.sd.ms.Ant0",
        "combo.aca.tp.noisy.sd.ms.Ant1",
    ]
    .map(|name| root.join("combo").join(name));
    let before = paths.iter().map(|path| tp_data(path)).collect::<Vec<_>>();
    request
        .parameters
        .insert("correlator".into(), json!(["BLC", "ACASpec"]));
    simalma(&request).unwrap();
    for (path, expected) in paths.iter().zip(before) {
        assert_eq!(tp_data(path), expected);
    }
}

fn tp_data(path: &std::path::Path) -> Vec<casa_core::ArrayValue> {
    let ms = casa_ms::MeasurementSet::open(path).unwrap();
    let main = ms.main_table();
    let rows = (0..main.row_count()).collect::<Vec<_>>();
    main.column_accessor("DATA")
        .unwrap()
        .array_cells_owned(&rows)
        .unwrap()
        .into_iter()
        .map(Option::unwrap)
        .collect()
}
