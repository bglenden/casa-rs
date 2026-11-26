// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use std::path::{Path, PathBuf};

use casa_calibration::{
    ApplyCalibrationTableSpec, CalibraterLifecycle, CalibraterLifecycleError,
    CalibrationOutputMode, CalibrationSolveRequest, CalibrationSolveResult, GainSolveCombine,
    GainSolveInterval, GainSolveMode, GainSolveModelSource, GainSolveRequest, GainType,
    RefAntSelector,
};
use casa_core::{ArrayValue, Complex32, Value};
use casa_ms::{MeasurementSet, MsSelection};
use casa_tables::{Table, TableOptions};
use ndarray::{ArrayD, IxDyn};

fn select_field(field: &str) -> MsSelection {
    MsSelection {
        field: Some(field.to_owned()),
        ..MsSelection::default()
    }
}

fn configure(calibrater: &mut CalibraterLifecycle, output: PathBuf) -> PathBuf {
    calibrater
        .set_solve(
            CalibrationSolveRequest::Gain(GainSolveRequest {
                selection: MsSelection::default(),
                output_table: output.clone(),
                gain_type: GainType::G,
                solve_mode: GainSolveMode::AmplitudePhase,
                solve_interval: GainSolveInterval::Infinite,
                preaverage_seconds: -1.0,
                correlation_dependent_flags: false,
                combine: GainSolveCombine::default(),
                refant: RefAntSelector::AntennaId(0),
                prior_calibration_tables: Vec::new(),
                parang: false,
                model_source: GainSolveModelSource::PointSource,
                normalize_average_amplitude: false,
                normalization_type: Default::default(),
                robustness: Default::default(),
                min_snr: 0.,
                min_baselines_per_antenna: 1,
                smodel: [0.; 4],
            }),
            CalibrationOutputMode::Replace,
        )
        .unwrap();
    output
}

fn gain_model(root: &Path, flux: f32) -> PathBuf {
    std::fs::create_dir_all(root).unwrap();
    common::create_gain_solve_model_column_fixture_ms(root, [Complex32::new(flux, 0.); 4])
}

fn gains(path: &Path) -> Vec<Complex32> {
    let table = Table::open(TableOptions::new(path)).unwrap();
    let column = table.column_accessor("CPARAM").unwrap();
    (0..table.row_count())
        .flat_map(|row| {
            let ArrayValue::Complex32(values) = column.array_cell(row).unwrap() else {
                panic!("a G solution must contain complex gains");
            };
            values.iter().copied().collect::<Vec<_>>()
        })
        .collect()
}

fn solve_without_product(calibrater: &CalibraterLifecycle, path: &Path) {
    let CalibrationSolveResult::Gain(report) = calibrater.solve().unwrap() else {
        panic!("a G solve must return a gain report");
    };
    assert_eq!(report.solution_row_count, 0);
    assert!(!path.exists());
}

fn expect_no_solver(calibrater: &CalibraterLifecycle, path: &Path) {
    assert!(matches!(
        calibrater.solve(),
        Err(CalibraterLifecycleError::NoSolver)
    ));
    assert!(!path.exists());
}

#[test]
fn tableless_opacity_prevents_public_rust_solve_from_writing() {
    let work = tempfile::tempdir().unwrap();
    let dataset = gain_model(&work.path().join("input"), 1.0);
    let mut calibrater = CalibraterLifecycle::default();
    calibrater.open(&dataset).unwrap();
    calibrater.select(select_field("0")).unwrap();
    let output = configure(&mut calibrater, work.path().join("gain.cal"));
    calibrater
        .set_tableless_opacity("TOpac", vec![0.1])
        .unwrap();
    assert!(
        matches!(calibrater.solve(), Err(CalibraterLifecycleError::ApplyConfiguration(message))
                     if message == "Error in TOpac::calcPar()")
    );
    assert!(!output.exists());
    calibrater.reset(true, false).unwrap();
    calibrater.solve().unwrap();
    assert!(output.exists());
}

fn exercise_model_lifecycle(ms: &Path, other: &Path, outputs: &Path, legacy: bool) {
    let mut calibrater = CalibraterLifecycle::default();
    assert!(calibrater.set_legacy_iterator(legacy));
    calibrater.open(ms).unwrap();
    calibrater.select(select_field("0")).unwrap();

    let stored = configure(&mut calibrater, outputs.join("stored.g"));
    calibrater.solve().unwrap();
    assert!(!gains(&stored).is_empty());

    calibrater.set_point_model(&[]).unwrap();
    let empty = configure(&mut calibrater, outputs.join("empty.g"));
    solve_without_product(&calibrater, &empty);
    calibrater.reset(true, true).unwrap();
    let reset = configure(&mut calibrater, outputs.join("reset.g"));
    solve_without_product(&calibrater, &reset);

    calibrater.set_point_model(&[1.]).unwrap();
    let positive = configure(&mut calibrater, outputs.join("positive.g"));
    calibrater.solve().unwrap();
    assert!(!gains(&positive).is_empty());

    calibrater.set_point_model(&[0., 0., 0., 0.]).unwrap();
    let zeros = configure(&mut calibrater, outputs.join("zeros.g"));
    solve_without_product(&calibrater, &zeros);
    let selected = configure(&mut calibrater, outputs.join("selected.g"));
    calibrater.select(select_field("0")).unwrap();
    expect_no_solver(&calibrater, &selected);
    let selected = configure(&mut calibrater, selected);
    solve_without_product(&calibrater, &selected);

    let abandoned = configure(&mut calibrater, outputs.join("abandoned.g"));
    assert!(calibrater.select(select_field("99999")).is_err());
    expect_no_solver(&calibrater, &abandoned);
    let recovered = configure(&mut calibrater, outputs.join("recovered.g"));
    calibrater.solve().unwrap();
    assert!(!gains(&recovered).is_empty());

    calibrater.set_point_model(&[]).unwrap();
    calibrater.close();
    calibrater.close();
    assert!(calibrater.dataset_path().is_none());
    assert!(matches!(
        calibrater.solve(),
        Err(CalibraterLifecycleError::Unattached)
    ));
    assert_eq!(calibrater.legacy_iterator(), legacy);
    calibrater.open(other).unwrap();
    assert_eq!(
        calibrater.dataset_path(),
        Some(other.canonicalize().unwrap().as_path())
    );
    assert_eq!(calibrater.legacy_iterator(), legacy);
    expect_no_solver(&calibrater, &abandoned);
    calibrater.select(select_field("0")).unwrap();
    let reopened = configure(&mut calibrater, outputs.join("reopened.g"));
    calibrater.solve().unwrap();
    assert!(!gains(&reopened).is_empty());
}

#[test]
fn calibrater_zero_point_models_follow_visibility_equation_transitions() {
    let root = tempfile::tempdir().unwrap();
    let ms = gain_model(&root.path().join("initial"), 2.);
    let other = gain_model(&root.path().join("second"), 8.);
    exercise_model_lifecycle(&ms, &other, root.path(), true);
    assert_eq!(
        gains(&root.path().join("stored.g")),
        gains(&root.path().join("recovered.g"))
    );
}

#[test]
#[ignore = "run with the installed CASA lifecycle oracle"]
fn installed_casa_model_lifecycle() {
    let root = PathBuf::from(std::env::var_os("CASARS_CALIBRATER_LIFECYCLE_WORK").unwrap());
    let outputs = root.join("native");
    std::fs::create_dir(&outputs).unwrap();
    exercise_model_lifecycle(
        &root.join("source-0.ms"),
        &root.join("source-1.ms"),
        &outputs,
        false,
    );
}

#[test]
fn calibrater_resets_apply_and_solve_independently() {
    let root = tempfile::tempdir().unwrap();
    let ms = gain_model(root.path(), 2.);
    let mut calibrater = CalibraterLifecycle::default();
    calibrater.open(&ms).unwrap();
    calibrater.select(select_field("0")).unwrap();
    let original = configure(&mut calibrater, root.path().join("original.g"));
    calibrater.solve().unwrap();
    let expected = gains(&original);

    calibrater
        .add_apply(ApplyCalibrationTableSpec::new(&original))
        .unwrap();
    let residual = configure(&mut calibrater, root.path().join("residual.g"));
    calibrater.reset(false, true).unwrap();
    expect_no_solver(&calibrater, &residual);
    let residual = configure(&mut calibrater, residual);
    calibrater.solve().unwrap();
    assert!(
        gains(&residual)
            .iter()
            .all(|gain| (*gain - Complex32::new(1., 0.)).norm() < 1e-4)
    );
    assert!(
        expected
            .iter()
            .any(|gain| (*gain - Complex32::new(1., 0.)).norm() > 1e-3)
    );

    let no_apply = configure(&mut calibrater, root.path().join("no-apply.g"));
    calibrater.reset(true, false).unwrap();
    calibrater.solve().unwrap();
    assert_eq!(gains(&no_apply), expected);

    calibrater
        .add_apply(ApplyCalibrationTableSpec::new(&original))
        .unwrap();
    let selected = configure(&mut calibrater, root.path().join("selected.g"));
    calibrater.select(select_field("0")).unwrap();
    expect_no_solver(&calibrater, &selected);
    let selected = configure(&mut calibrater, selected);
    calibrater.solve().unwrap();
    assert_eq!(gains(&selected), expected);
}

fn samples(ms: &Path, column: &str) -> Vec<ArrayD<Complex32>> {
    let ms = MeasurementSet::open(ms).unwrap();
    let main = ms.main_table();
    let cells = main.column_accessor(column).unwrap();
    (0..main.row_count())
        .map(|row| {
            let ArrayValue::Complex32(values) = cells.array_cell(row).unwrap() else {
                panic!("a visibility cell must be complex");
            };
            values.clone()
        })
        .collect()
}

fn place_corrected_sentinel(path: &Path, value: Complex32) {
    let mut ms = MeasurementSet::open(path).unwrap();
    let main = ms.main_table_mut();
    for row in 0..main.row_count() {
        let data = ArrayValue::Complex32(ArrayD::from_elem(IxDyn(&[2, 2]), value));
        main.cell_accessor_mut(row, "CORRECTED_DATA")
            .unwrap()
            .set(Value::Array(data))
            .unwrap();
    }
    ms.save().unwrap();
}

#[test]
fn calibrater_keeps_the_legacy_iterator_across_reopens() {
    let root = tempfile::tempdir().unwrap();
    let first_dir = root.path().join("first");
    let second_dir = root.path().join("second");
    std::fs::create_dir(&first_dir).unwrap();
    std::fs::create_dir(&second_dir).unwrap();
    let first = common::create_apply_fixture_ms(&first_dir, true);
    let second = common::create_apply_fixture_ms(&second_dir, true);
    let mut calibrater = CalibraterLifecycle::default();
    calibrater.open(&first).unwrap();
    assert!(!calibrater.set_legacy_iterator(true));
    calibrater.close();
    calibrater.open(&second).unwrap();
    assert!(
        calibrater
            .init_cal_set(0)
            .unwrap_err()
            .to_string()
            .contains("not implemented")
    );
    calibrater.close();

    assert!(calibrater.set_legacy_iterator(true));
    for (path, other, sentinel) in [
        (&first, &second, Complex32::new(3., 7.)),
        (&second, &first, Complex32::new(5., -9.)),
        (&first, &second, Complex32::new(-4., 3.)),
    ] {
        place_corrected_sentinel(path, sentinel);
        let other_before = samples(other, "CORRECTED_DATA");
        let data = samples(path, "DATA");
        let selection = MsSelection {
            spw: Some("0:1".into()),
            ..select_field("0")
        };
        calibrater.open(path).unwrap();
        calibrater.select(selection).unwrap();
        calibrater.init_cal_set(0).unwrap();
        calibrater.close();
        let corrected = samples(path, "CORRECTED_DATA");
        for (row, values) in corrected.iter().enumerate() {
            for (index, &value) in values.indexed_iter() {
                let expected = if row == 0 && index[1] == 1 {
                    data[row][index]
                } else {
                    sentinel
                };
                assert_eq!(value, expected);
            }
        }
        assert_eq!(samples(other, "CORRECTED_DATA"), other_before);
    }
}
