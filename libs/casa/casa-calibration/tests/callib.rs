// SPDX-License-Identifier: LGPL-3.0-or-later

use crate::common;

use tempfile::TempDir;

use casa_calibration::{
    apply::{ApplyMode, ApplyPlanRequest, execute_apply_from_path, plan_apply_from_path},
    callib::load_apply_specs_from_callib,
};
use casa_core::ArrayValue;
use casa_ms::MeasurementSet;
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::selection::MsSelection;

#[test]
fn plan_apply_from_callib_tracks_per_table_applicability_and_absolute_paths() {
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
fn execute_apply_from_callib_respects_field_selectors_and_absolute_paths() {
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

    assert_eq!(row0[[0, 0]], casa_core::Complex32::new(0.5, 0.0));
    assert_eq!(row1[[0, 0]], casa_core::Complex32::new(0.05, 0.0));
}

fn create_two_table_callib_fixture(
    root: &std::path::Path,
) -> (std::path::PathBuf, std::path::PathBuf) {
    let ms_path = common::create_apply_fixture_ms(root, false);
    // CLPatchPanel arranges all metadata antennas. The older minimal MS helper
    // leaves ANTENNA empty despite MAIN referring to IDs 0/1; complete this
    // fixture's metadata without changing DATA or the analytic gain values.
    let mut ms = MeasurementSet::open(&ms_path).unwrap();
    let antennas = ms
        .subtable_mut(casa_ms::schema::SubtableId::Antenna)
        .unwrap();
    for id in 0..2 {
        antennas
            .add_row(common::make_subtable_row(
                casa_ms::schema::antenna::REQUIRED_COLUMNS,
                &[
                    (
                        "NAME",
                        casa_core::Value::Scalar(casa_core::ScalarValue::String(format!("A{id}"))),
                    ),
                    (
                        "FLAG_ROW",
                        casa_core::Value::Scalar(casa_core::ScalarValue::Bool(false)),
                    ),
                ],
            ))
            .unwrap();
    }
    ms.save().unwrap();
    drop(ms);

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
                    casa_core::Complex32::new(1.0, 0.0),
                    casa_core::Complex32::new(1.0, 0.0),
                ],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 1,
                gains: vec![
                    casa_core::Complex32::new(2.0, 0.0),
                    casa_core::Complex32::new(4.0, 0.0),
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
                    casa_core::Complex32::new(2.0, 0.0),
                    casa_core::Complex32::new(2.0, 0.0),
                ],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 200.0,
                field_id: 1,
                spectral_window_id: 1,
                antenna_id: 1,
                gains: vec![
                    casa_core::Complex32::new(10.0, 0.0),
                    casa_core::Complex32::new(20.0, 0.0),
                ],
                flags: vec![false, false],
            },
        ],
    );

    let callib_path = root.join("apply.callib");
    std::fs::write(
        &callib_path,
        format!(
            "caltable='{}' field='0' calwt=F tinterp='nearest'\ncaltable='{}' field='1' calwt=F tinterp='nearest'\n",
            root.join("field0.gcal").display(), root.join("field1.gcal").display()
        ),
    )
    .expect("write callib");
    (ms_path, callib_path)
}

#[test]
fn library_antenna_broadcast_uses_the_mapped_gain_for_both_partners() {
    let dir = TempDir::new().unwrap();
    let (ms_path, callib) = create_two_table_callib_fixture(dir.path());
    std::fs::write(
        &callib,
        format!(
            "caltable='{}' field='0' antmap=[1] calwt=False\n",
            dir.path().join("field0.gcal").display()
        ),
    )
    .unwrap();
    let request = ApplyPlanRequest {
        selection: MsSelection::new(),
        apply_mode: ApplyMode::CalOnly,
        parang: false,
        calibration_tables: load_apply_specs_from_callib(&callib).unwrap(),
    };
    execute_apply_from_path(&ms_path, &request).unwrap();
    let ms = MeasurementSet::open(&ms_path).unwrap();
    let corrected = ms.data_column(VisibilityDataColumn::CorrectedData).unwrap();
    let ArrayValue::Complex32(row) = corrected.get(0).unwrap() else {
        panic!("complex");
    };
    // DATA=1 and both mapped R gains are 2: V/(2*conj(2))=1/4.
    assert_eq!(row[[0, 0]], casa_core::Complex32::new(0.25, 0.));
}

#[test]
fn library_joint_context_preserves_valid_samples_and_skips_absent_axes() {
    for (scan_map, mode, expected, flagged) in [
        ("[1,1]", ApplyMode::CalFlag, 0.5, false),
        ("[999,1]", ApplyMode::CalFlag, 1., false),
        ("[999,1]", ApplyMode::CalFlagStrict, 1., true),
    ] {
        let dir = TempDir::new().unwrap();
        let (ms_path, callib) = create_two_table_callib_fixture(dir.path());
        std::fs::write(
            &callib,
            format!(
                "caltable='{}' field='0' obsmap='self' scanmap={scan_map} calwt=False\n",
                dir.path().join("field0.gcal").display()
            ),
        )
        .unwrap();
        execute_apply_from_path(
            &ms_path,
            &ApplyPlanRequest {
                selection: MsSelection::new(),
                apply_mode: mode,
                parang: false,
                calibration_tables: load_apply_specs_from_callib(&callib).unwrap(),
            },
        )
        .unwrap();
        let ms = MeasurementSet::open(&ms_path).unwrap();
        let corrected = ms.data_column(VisibilityDataColumn::CorrectedData).unwrap();
        let ArrayValue::Complex32(row) = corrected.get(0).unwrap() else {
            panic!("complex");
        };
        assert_eq!(row[[0, 0]], casa_core::Complex32::new(expected, 0.));
        let flags = ms
            .main_table()
            .cell_accessor(0, "FLAG")
            .unwrap()
            .array()
            .unwrap();
        let ArrayValue::Bool(flags) = flags else {
            panic!("flags");
        };
        assert!(flags.iter().all(|value| *value == flagged));
    }
}

#[test]
fn library_with_no_mapped_antennas_passes_without_flagging() {
    let dir = TempDir::new().unwrap();
    let (ms_path, callib) = create_two_table_callib_fixture(dir.path());
    std::fs::write(
        &callib,
        format!(
            "caltable='{}' field='0' antmap=[999]\n",
            dir.path().join("field0.gcal").display()
        ),
    )
    .unwrap();
    execute_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new(),
            apply_mode: ApplyMode::CalFlagStrict,
            parang: false,
            calibration_tables: load_apply_specs_from_callib(&callib).unwrap(),
        },
    )
    .unwrap();
    let ms = MeasurementSet::open(&ms_path).unwrap();
    let ArrayValue::Bool(flags) = ms
        .main_table()
        .cell_accessor(0, "FLAG")
        .unwrap()
        .array()
        .unwrap()
    else {
        panic!("flags");
    };
    assert!(flags.iter().all(|flag| !*flag));
}
