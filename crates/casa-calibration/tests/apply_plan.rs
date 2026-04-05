// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use tempfile::TempDir;

use casa_calibration::{
    ApplyCalibrationTableSpec, ApplyInterpolationMode, ApplyMode, ApplyPlanRequest,
    ApplyTableSelection, GainFieldSelector, plan_apply_from_path,
};
use casacore_ms::selection::MsSelection;

#[test]
fn plan_apply_marks_corrected_data_creation_when_absent() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_apply_fixture_ms(dir.path(), false);
    let caltable_path = common::create_apply_fixture_caltable(
        &dir.path().join("phase.gcal"),
        &["TARGET0", "TARGET1"],
        &[0, 1],
        &[0, 1],
    );

    let plan = plan_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new(),
            apply_mode: ApplyMode::CalFlag,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&caltable_path)],
        },
    )
    .expect("plan apply");

    assert!(plan.requires_corrected_data_column);
    assert_eq!(plan.selected_row_count, 2);
    assert_eq!(plan.selected_rows.len(), 2);
    assert_eq!(plan.selected_data_spw_ids, vec![0, 1]);
}

#[test]
fn plan_apply_resolves_spwmap_for_selected_rows() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_apply_fixture_ms(dir.path(), true);
    let caltable_path = common::create_apply_fixture_caltable(
        &dir.path().join("phase.gcal"),
        &["TARGET0"],
        &[0],
        &[0],
    );

    let plan = plan_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new().spw(&[1]),
            apply_mode: ApplyMode::Trial,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec {
                path: caltable_path.clone(),
                apply_to: Default::default(),
                gainfield: None,
                spwmap: vec![0, 0],
                interp: ApplyInterpolationMode::NearestLinear,
                calwt: false,
            }],
        },
    )
    .expect("plan apply");

    assert_eq!(plan.selected_row_count, 1);
    assert_eq!(plan.selected_data_spw_ids, vec![1]);
    assert_eq!(plan.calibration_tables.len(), 1);
    assert_eq!(plan.calibration_tables[0].spw_mapping.len(), 1);
    assert_eq!(plan.calibration_tables[0].spw_mapping[0].data_spw_id, 1);
    assert_eq!(
        plan.calibration_tables[0].spw_mapping[0].calibration_spw_id,
        0
    );
    assert!(plan.calibration_tables[0].interp.uses_frequency_axis());
}

#[test]
fn plan_apply_resolves_gainfield_by_exact_name() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_apply_fixture_ms(dir.path(), true);
    let caltable_path = common::create_apply_fixture_caltable(
        &dir.path().join("phase.gcal"),
        &["CALIBRATOR", "TARGET1"],
        &[0, 1],
        &[0, 1],
    );

    let plan = plan_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new(),
            apply_mode: ApplyMode::CalOnly,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec {
                path: caltable_path,
                apply_to: Default::default(),
                gainfield: Some(GainFieldSelector::FieldName("CALIBRATOR".to_string())),
                spwmap: Vec::new(),
                interp: ApplyInterpolationMode::Nearest,
                calwt: true,
            }],
        },
    )
    .expect("plan apply");

    let resolved = plan.calibration_tables[0]
        .resolved_gainfield
        .as_ref()
        .expect("resolved gainfield");
    assert_eq!(resolved.field_id, 0);
    assert_eq!(resolved.field_name.as_deref(), Some("CALIBRATOR"));
    assert!(plan.calibration_tables[0].calwt);
}

#[test]
fn plan_apply_resolves_gainfield_nearest_by_field_direction() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_apply_fixture_ms(dir.path(), true);
    common::set_ms_field_directions(&ms_path, &[(0, 1.19, 0.5), (1, 1.20, 0.6)]);

    let caltable_path = common::create_apply_fixture_caltable(
        &dir.path().join("nearest.gcal"),
        &["CAL0", "CAL1"],
        &[0, 1],
        &[0, 1],
    );
    common::set_caltable_field_directions(&caltable_path, &[(0, 1.0, 0.5), (1, 1.2, 0.5)]);

    let plan = plan_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            apply_mode: ApplyMode::Trial,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec {
                path: caltable_path,
                apply_to: Default::default(),
                gainfield: Some(GainFieldSelector::Nearest),
                spwmap: Vec::new(),
                interp: ApplyInterpolationMode::Nearest,
                calwt: false,
            }],
        },
    )
    .expect("plan apply nearest gainfield");

    let resolved = &plan.calibration_tables[0].resolved_nearest_gainfields;
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].measurement_set_field_id, 0);
    assert_eq!(resolved[0].calibration_field_id, 1);
    assert_eq!(resolved[0].calibration_field_name.as_deref(), Some("CAL1"));
}

#[test]
fn plan_apply_accepts_k_jones_delay_tables() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_apply_fixture_ms(dir.path(), true);
    let caltable_path = common::create_apply_delay_caltable(
        &dir.path().join("delay.kcal"),
        &["TARGET0"],
        &[
            common::SyntheticDelaySolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 0,
                delays_ns: vec![250.0, 250.0],
                flags: vec![false, false],
            },
            common::SyntheticDelaySolutionRow {
                time_seconds: 100.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 1,
                delays_ns: vec![0.0, 0.0],
                flags: vec![false, false],
            },
        ],
    );

    let plan = plan_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            apply_mode: ApplyMode::Trial,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&caltable_path)],
        },
    )
    .expect("plan apply for K Jones");

    assert_eq!(plan.selected_row_count, 1);
    assert_eq!(plan.calibration_tables.len(), 1);
    assert_eq!(
        plan.calibration_tables[0].summary.table_subtype,
        "K Jones".to_string()
    );
    assert_eq!(plan.calibration_tables[0].spw_mapping.len(), 1);
}

#[test]
fn plan_apply_accepts_bpoly_tables() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_apply_fixture_ms(dir.path(), true);
    let bpoly_path = common::create_apply_bpoly_caltable(
        &dir.path().join("bandpass.bpoly"),
        &[common::SyntheticBPolySolutionRow {
            time_seconds: 100.0,
            field_id: 0,
            spectral_window_id: 0,
            antenna_id: 0,
            scale_factor: casacore_types::Complex32::new(1.0, 0.0),
            valid_domain_hz: [1.0e9, 1.001e9],
            amp_coefficients: vec![vec![0.0, 0.0], vec![0.0, 0.0]],
            phase_coefficients: vec![vec![0.0], vec![0.0]],
            phase_units: "RADIANS",
        }],
    );

    let plan = plan_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            apply_mode: ApplyMode::Trial,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&bpoly_path)],
        },
    )
    .expect("plan apply for BPOLY");

    assert_eq!(plan.calibration_tables[0].summary.table_subtype, "BPOLY");
    assert_eq!(plan.calibration_tables[0].spw_mapping.len(), 1);
}

#[test]
fn plan_apply_rejects_explicit_gainfield_for_bpoly_tables() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_apply_fixture_ms(dir.path(), true);
    let bpoly_path = common::create_apply_bpoly_caltable(
        &dir.path().join("bandpass.bpoly"),
        &[common::SyntheticBPolySolutionRow {
            time_seconds: 100.0,
            field_id: 0,
            spectral_window_id: 0,
            antenna_id: 0,
            scale_factor: casacore_types::Complex32::new(1.0, 0.0),
            valid_domain_hz: [1.0e9, 1.001e9],
            amp_coefficients: vec![vec![0.0, 0.0], vec![0.0, 0.0]],
            phase_coefficients: vec![vec![0.0], vec![0.0]],
            phase_units: "RADIANS",
        }],
    );

    let error = plan_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            apply_mode: ApplyMode::Trial,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec {
                path: bpoly_path,
                apply_to: Default::default(),
                gainfield: Some(GainFieldSelector::FieldId(0)),
                spwmap: Vec::new(),
                interp: ApplyInterpolationMode::Nearest,
                calwt: false,
            }],
        },
    )
    .expect_err("BPOLY gainfield should be rejected");

    assert!(error
        .to_string()
        .contains("BPOLY apply currently supports only the default field mapping"));
}

#[test]
fn plan_apply_tracks_per_table_applicability_selection() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_apply_fixture_ms(dir.path(), true);
    let caltable_path = common::create_apply_fixture_caltable(
        &dir.path().join("phase.gcal"),
        &["TARGET0", "TARGET1"],
        &[0, 1],
        &[0, 1],
    );

    let plan = plan_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new(),
            apply_mode: ApplyMode::Trial,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec {
                path: caltable_path,
                apply_to: ApplyTableSelection {
                    field_ids: vec![1],
                    spectral_window_ids: Vec::new(),
                    observation_ids: Vec::new(),
                },
                gainfield: None,
                spwmap: Vec::new(),
                interp: ApplyInterpolationMode::Nearest,
                calwt: false,
            }],
        },
    )
    .expect("plan apply with per-table selection");

    assert_eq!(plan.selected_row_count, 2);
    assert_eq!(plan.calibration_tables[0].applicable_selected_row_count, 1);
    assert_eq!(plan.calibration_tables[0].spw_mapping.len(), 1);
    assert_eq!(plan.calibration_tables[0].spw_mapping[0].data_spw_id, 1);
    assert_eq!(plan.calibration_tables[0].spec.apply_to.field_ids, vec![1]);
}

#[test]
fn plan_apply_allows_parang_without_caltables() {
    let dir = TempDir::new().expect("tempdir");
    let ms_path = common::create_apply_fixture_ms(dir.path(), true);

    let plan = plan_apply_from_path(
        &ms_path,
        &ApplyPlanRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            apply_mode: ApplyMode::Trial,
            parang: true,
            calibration_tables: Vec::new(),
        },
    )
    .expect("plan parang-only apply");

    assert!(plan.parang);
    assert!(plan.calibration_tables.is_empty());
    assert_eq!(plan.selected_row_count, 1);
    assert_eq!(plan.selected_field_ids, vec![0]);
}
