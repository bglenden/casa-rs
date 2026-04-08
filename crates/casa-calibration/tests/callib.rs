// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use tempfile::TempDir;

use casa_calibration::{
    ApplyMode, ApplyPlanRequest, execute_apply_from_path, load_apply_specs_from_callib,
    plan_apply_from_path,
};
use casa_ms::ms::MeasurementSet;
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::selection::MsSelection;
use casa_types::ArrayValue;

#[test]
fn plan_apply_from_callib_tracks_per_table_applicability_and_relative_paths() {
    let dir = TempDir::new().expect("tempdir");
    let (ms_path, callib_path) = create_two_table_callib_fixture(dir.path());

    let specs = load_apply_specs_from_callib(&callib_path).expect("load callib");
    assert_eq!(specs[0].path, dir.path().join("field0.gcal"));
    assert_eq!(specs[1].path, dir.path().join("field1.gcal"));

    let plan = plan_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new(),
            apply_mode: ApplyMode::Trial,
            parang: false,
            calibration_tables: specs,
        },
    )
    .expect("plan apply from callib");

    assert_eq!(plan.selected_row_count, 2);
    assert_eq!(plan.calibration_tables.len(), 2);
    assert_eq!(plan.calibration_tables[0].applicable_selected_row_count, 1);
    assert_eq!(plan.calibration_tables[1].applicable_selected_row_count, 1);
    assert_eq!(plan.calibration_tables[0].spw_mapping[0].data_spw_id, 0);
    assert_eq!(plan.calibration_tables[1].spw_mapping[0].data_spw_id, 1);
}

#[test]
fn execute_apply_from_callib_respects_field_selectors_and_relative_paths() {
    let dir = TempDir::new().expect("tempdir");
    let (ms_path, callib_path) = create_two_table_callib_fixture(dir.path());

    let specs = load_apply_specs_from_callib(&callib_path).expect("load callib");
    execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new(),
            apply_mode: ApplyMode::CalOnly,
            parang: false,
            calibration_tables: specs,
        },
    )
    .expect("apply from callib");

    let ms = MeasurementSet::open(&ms_path).expect("reopen measurement set");
    let corrected = ms
        .data_column(VisibilityDataColumn::CorrectedData)
        .expect("corrected data accessor");
    let row0 = corrected.get(0).expect("corrected row 0");
    let row1 = corrected.get(1).expect("corrected row 1");
    let ArrayValue::Complex32(row0) = row0 else {
        panic!("expected complex corrected row 0");
    };
    let ArrayValue::Complex32(row1) = row1 else {
        panic!("expected complex corrected row 1");
    };

    assert_eq!(row0[[0, 0]], casa_types::Complex32::new(0.5, 0.0));
    assert_eq!(row1[[0, 0]], casa_types::Complex32::new(0.05, 0.0));
}

fn create_two_table_callib_fixture(
    root: &std::path::Path,
) -> (std::path::PathBuf, std::path::PathBuf) {
    let ms_path = common::create_apply_fixture_ms(root, false);
    common::create_apply_gain_caltable(
        &root.join("field0.gcal"),
        &["TARGET0"],
        &[
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 0,
                gains: vec![
                    casa_types::Complex32::new(1.0, 0.0),
                    casa_types::Complex32::new(1.0, 0.0),
                ],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 1,
                gains: vec![
                    casa_types::Complex32::new(2.0, 0.0),
                    casa_types::Complex32::new(4.0, 0.0),
                ],
                flags: vec![false, false],
            },
        ],
    );
    common::create_apply_gain_caltable(
        &root.join("field1.gcal"),
        &["TARGET1"],
        &[
            common::SyntheticGainSolutionRow {
                time_seconds: 200.0,
                field_id: 1,
                spectral_window_id: 1,
                antenna_id: 0,
                gains: vec![
                    casa_types::Complex32::new(2.0, 0.0),
                    casa_types::Complex32::new(2.0, 0.0),
                ],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 200.0,
                field_id: 1,
                spectral_window_id: 1,
                antenna_id: 1,
                gains: vec![
                    casa_types::Complex32::new(10.0, 0.0),
                    casa_types::Complex32::new(20.0, 0.0),
                ],
                flags: vec![false, false],
            },
        ],
    );

    let callib_path = root.join("apply.callib");
    std::fs::write(
        &callib_path,
        "\
caltable='field0.gcal' field='0' calwt=F tinterp='nearest'\n\
caltable='field1.gcal' field='1' calwt=F tinterp='nearest'\n",
    )
    .expect("write callib");
    (ms_path, callib_path)
}
