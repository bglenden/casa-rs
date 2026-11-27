// SPDX-License-Identifier: LGPL-3.0-or-later
//! Laws observed with CASA 6.7.6.14 and its storeNCT/specify implementation.
#[path = "../../../libs/casa/casa-calibration/tests/common/mod.rs"]
mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_calibration::bandpass::{BandpassSolveCombine, BandpassSolveRequest, BandpassType};
use casa_calibration::corrected_export::{
    ExportCorrectedDataError, ExportCorrectedDataRequest, export_corrected_data,
};
use casa_calibration::gencal::{GencalRequest, GencalType, gencal};
use casa_calibration::output::CalibrationOutputMode;
use casa_calibration::solve::{
    CalibrationDataset, CalibrationError, CalibrationSolveRequest, CalibrationSolveResult,
    GainSolveCombine, GainSolveInterval, GainSolveMode, GainSolveModelSource, GainSolveRequest,
    GainType, RefAntSelector, solve_calibration_with_output_mode,
};
use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};
use casa_ms::MsSelection;
use casa_tables::{Table, TableOptions};
use tempfile::TempDir;

fn gain(path: &Path) -> GainSolveRequest {
    GainSolveRequest {
        selection: MsSelection::new(),
        output_table: path.to_path_buf(),
        gain_type: GainType::G,
        solve_mode: GainSolveMode::Phase,
        solve_interval: GainSolveInterval::Infinite,
        combine: GainSolveCombine::default(),
        refant: RefAntSelector::AntennaId(0),
        prior_calibration_tables: vec![],
        parang: false,
        model_source: GainSolveModelSource::PointSource,
        normalize_average_amplitude: false,
        preaverage_seconds: 0.0,
        correlation_dependent_flags: false,
        normalization_type: Default::default(),
        robustness: Default::default(),
        min_snr: 0.0,
        min_baselines_per_antenna: 0,
        smodel: [1.0, 0.0, 0.0, 0.0],
    }
}

fn solve(
    ms: &Path,
    request: GainSolveRequest,
    mode: CalibrationOutputMode,
) -> Result<CalibrationSolveResult, CalibrationError> {
    solve_calibration_with_output_mode(
        CalibrationDataset::path(ms),
        CalibrationSolveRequest::Gain(request),
        mode,
    )
}

fn table(path: &Path) -> Table {
    Table::open(TableOptions::new(path)).unwrap()
}
fn rows(path: &Path) -> Vec<RecordValue> {
    table(path).rows().unwrap().to_vec()
}
fn snapshot(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(root: &Path, p: &Path, result: &mut BTreeMap<PathBuf, Vec<u8>>) {
        if p.is_dir() {
            for entry in std::fs::read_dir(p).unwrap() {
                walk(root, &entry.unwrap().path(), result);
            }
        } else {
            result.insert(
                p.strip_prefix(root).unwrap().to_path_buf(),
                std::fs::read(p).unwrap(),
            );
        }
    }
    let mut result = BTreeMap::new();
    walk(path, path, &mut result);
    result
}

#[test]
fn replace_and_append_then_no_solutions_preserve_the_product() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let output = dir.path().join("g.cal");
    solve(&ms, gain(&output), CalibrationOutputMode::Replace).unwrap();
    let first = rows(&output);
    solve(&ms, gain(&output), CalibrationOutputMode::Append).unwrap();
    let appended = rows(&output);
    assert_eq!(appended.len(), 2 * first.len());
    // Each original solution appears twice, sorted by CASA's disk keys.
    for row in &first {
        assert_eq!(appended.iter().filter(|r| *r == row).count(), 2);
    }
    solve(&ms, gain(&output), CalibrationOutputMode::Replace).unwrap();
    assert_eq!(rows(&output), first);
    let before = snapshot(&output);
    let mut none = gain(&output);
    none.min_baselines_per_antenna = 100;
    let CalibrationSolveResult::Gain(report) =
        solve(&ms, none.clone(), CalibrationOutputMode::Replace).unwrap()
    else {
        panic!("gain report")
    };
    assert_eq!(report.solution_row_count, 0);
    assert_eq!(snapshot(&output), before);
    none.output_table = dir.path().join("absent.cal");
    solve(&ms, none.clone(), CalibrationOutputMode::Append).unwrap();
    assert!(!none.output_table.exists());
}

#[test]
fn failed_pipelined_input_finishes_and_preserves_the_prior_product() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let output = dir.path().join("g.cal");
    solve(&ms, gain(&output), CalibrationOutputMode::Replace).unwrap();
    let before = snapshot(&output);
    let mut main = table(&ms);
    let template = main.rows().unwrap()[0].clone();
    while main.row_count() < 1024 {
        main.add_row(template.clone()).unwrap();
    }
    main.cell_accessor_mut(300, "FLAG")
        .unwrap()
        .set(Value::Array(ArrayValue::Bool(ndarray::ArrayD::from_elem(
            ndarray::IxDyn(&[1, 1]),
            false,
        ))))
        .unwrap();
    main.save(TableOptions::new(&ms)).unwrap();
    drop(main);
    let request = dir.path().join("request.json");
    std::fs::write(
        &request,
        serde_json::to_vec(&serde_json::json!({
            "surface": "gaincal",
            "values": {"vis": ms, "caltable": output, "append": true,
                "refant": "0", "smodel": [1], "minsnr": 0, "minblperant": 0}
        }))
        .unwrap(),
    )
    .unwrap();
    let stderr = dir.path().join("stderr");
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_calibrate"))
        .arg("--json-run")
        .arg(request)
        .env("RAYON_NUM_THREADS", "2")
        .stdout(std::process::Stdio::null())
        .stderr(std::fs::File::create(&stderr).unwrap())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if std::time::Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("failed input must release a blocked batch reader");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    };
    assert!(!status.success());
    assert!(std::fs::read_to_string(stderr).unwrap().contains("shape"));
    assert_eq!(snapshot(&output), before);
}

#[test]
fn incompatible_append_and_interrupted_staging_preserve_every_prior_byte() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let output = dir.path().join("g.cal");
    solve(&ms, gain(&output), CalibrationOutputMode::Replace).unwrap();
    let before = snapshot(&output);
    let mut incompatible = gain(&output);
    incompatible.gain_type = GainType::T;
    assert!(
        solve(&ms, incompatible, CalibrationOutputMode::Append)
            .unwrap_err()
            .to_string()
            .contains("VisCal")
    );
    assert_eq!(snapshot(&output), before);
    let interrupted = dir
        .path()
        .join(format!(".g.cal.casa-rs-incomplete-{}", std::process::id()));
    std::fs::create_dir(&interrupted).unwrap();
    std::fs::write(interrupted.join("receipt"), "interrupted predecessor").unwrap();
    assert!(
        solve(&ms, gain(&output), CalibrationOutputMode::Replace)
            .unwrap_err()
            .to_string()
            .contains("interrupted")
    );
    assert_eq!(
        std::fs::read_to_string(interrupted.join("receipt")).unwrap(),
        "interrupted predecessor"
    );
    assert_eq!(snapshot(&output), before);
}

#[test]
fn append_rejects_spectral_metadata_changes_and_preserves_subtable_keywords() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let output = dir.path().join("g.cal");
    solve(&ms, gain(&output), CalibrationOutputMode::Replace).unwrap();
    let spw = output.join("SPECTRAL_WINDOW");
    let mut metadata = table(&spw);
    metadata
        .cell_accessor_mut(0, "REF_FREQUENCY")
        .unwrap()
        .set(Value::Scalar(ScalarValue::Float64(-1.0)))
        .unwrap();
    metadata.save(TableOptions::new(&spw)).unwrap();
    let before = snapshot(&output);
    assert!(
        solve(&ms, gain(&output), CalibrationOutputMode::Append)
            .unwrap_err()
            .to_string()
            .contains("REF_FREQUENCY")
    );
    assert_eq!(snapshot(&output), before);
}

#[test]
fn generated_corrections_accumulate_by_selected_antenna_and_refuse_wrong_types() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let output = dir.path().join("o.cal");
    let mut request = GencalRequest {
        measurement_set: ms,
        output_table: output.clone(),
        caltype: GencalType::Opac,
        antenna: String::new(),
        spw: String::new(),
        parameter: vec![0.05],
        gaincurve_table: None,
    };
    gencal(&request).unwrap();
    let mut prior = table(&output);
    for (column, value) in [
        (
            "FLAG",
            Value::Array(ArrayValue::Bool(ndarray::ArrayD::from_elem(
                ndarray::IxDyn(&[1, 1]),
                true,
            ))),
        ),
        (
            "SNR",
            Value::Array(ArrayValue::Float32(ndarray::ArrayD::from_elem(
                ndarray::IxDyn(&[1, 1]),
                5.0,
            ))),
        ),
        (
            "PARAMERR",
            Value::Array(ArrayValue::Float32(ndarray::ArrayD::from_elem(
                ndarray::IxDyn(&[1, 1]),
                0.2,
            ))),
        ),
    ] {
        prior
            .cell_accessor_mut(0, column)
            .unwrap()
            .set(value)
            .unwrap();
    }
    prior.save(TableOptions::new(&output)).unwrap();
    let preserved = rows(&output)[0].clone();

    request.antenna = "1".to_string();
    request.parameter = vec![0.12];
    gencal(&request).unwrap();
    gencal(&request).unwrap();
    for row in rows(&output) {
        let Value::Array(ArrayValue::Float32(p)) = row.get("FPARAM").unwrap() else {
            panic!("FPARAM")
        };
        let expected = if row.get("ANTENNA1") == Some(&Value::Scalar(ScalarValue::Int32(1))) {
            0.29
        } else {
            0.05
        };
        assert!((p.iter().next().unwrap() - expected).abs() < 1e-6);
    }
    let after = rows(&output);
    for column in ["FLAG", "SNR", "PARAMERR"] {
        assert_eq!(after[0].get(column), preserved.get(column));
    }
    assert!(
        after
            .iter()
            .all(|r| r.get("OBSERVATION_ID") == Some(&Value::Scalar(ScalarValue::Int32(-1))))
    );
    let before = snapshot(&output);
    request.caltype = GencalType::Antpos;
    request.parameter = vec![0.01, 0.02, 0.03];
    assert!(gencal(&request).is_err());
    assert_eq!(snapshot(&output), before);
}

#[test]
fn replacing_an_ms_or_a_non_table_is_refused() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let before = snapshot(&ms);
    assert!(solve(&ms, gain(&ms), CalibrationOutputMode::Replace).is_err());
    assert_eq!(snapshot(&ms), before);
    let output = dir.path().join("note");
    std::fs::write(&output, "keep").unwrap();
    assert!(solve(&ms, gain(&output), CalibrationOutputMode::Replace).is_err());
    assert_eq!(std::fs::read_to_string(&output).unwrap(), "keep");
}

#[test]
fn report_aliasing_a_table_file_is_refused_before_replacement() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let output = dir.path().join("g.cal");
    solve(&ms, gain(&output), CalibrationOutputMode::Replace).unwrap();
    let before = snapshot(&output);
    let result = std::process::Command::new(env!("CARGO_BIN_EXE_calibrate"))
        .args(["gaincal", "--vis"])
        .arg(&ms)
        .arg("--caltable")
        .arg(&output)
        .args([
            "--refant",
            "0",
            "--minsnr",
            "0",
            "--minblperant",
            "0",
            "--overwrite",
            "--no-save-last",
            "--output",
        ])
        .arg(output.join("table.dat"))
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("overlaps"));
    assert_eq!(snapshot(&output), before);
}

#[test]
fn polynomial_bandpass_append_reuses_the_window_descriptor() {
    let dir = TempDir::new().unwrap();
    let unit = casa_core::Complex32::new(1.0, 0.0);
    let ms = common::create_bandpass_solve_fixture_ms(
        dir.path(),
        &[common::SyntheticBandpassTimeCluster {
            field_id: 0,
            time_seconds: 100.0,
            scan_number: 1,
            prior_gains: [[unit; 2]; 3],
            bandpass_gains: [[[unit; 2]; 2]; 3],
        }],
    );
    let output = dir.path().join("poly.cal");
    let request = BandpassSolveRequest {
        model_source: Default::default(),
        solve_interval: Default::default(),
        frequency_interval: None,
        correlation_dependent_flags: false,
        selection: MsSelection::new(),
        output_table: output.clone(),
        refant: RefAntSelector::AntennaId(0),
        prior_calibration_tables: vec![],
        parang: false,
        combine: BandpassSolveCombine::default(),
        band_type: BandpassType::BPoly,
        normalize_average_amplitude: false,
        amplitude_degree: 1,
        phase_degree: 1,
        smodel: [1.0, 0.0, 0.0, 0.0],
        cuts: None,
    };
    let run = |mode| {
        solve_calibration_with_output_mode(
            CalibrationDataset::path(&ms),
            CalibrationSolveRequest::Bandpass(request.clone()),
            mode,
        )
        .unwrap()
    };
    run(CalibrationOutputMode::Replace);
    let first = rows(&output);
    run(CalibrationOutputMode::Append);
    let all = rows(&output);
    assert_eq!(all.len(), 2 * first.len());
    assert_eq!(&all[..first.len()], first.as_slice());
    assert_eq!(&all[first.len()..], first.as_slice());
    assert_eq!(table(&output.join("CAL_DESC")).row_count(), 1);
}

#[test]
fn append_refuses_a_missing_parameter_column_without_changing_the_table() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let output = dir.path().join("g.cal");
    solve(&ms, gain(&output), CalibrationOutputMode::Replace).unwrap();
    let mut malformed = table(&output);
    malformed.remove_column("CPARAM").unwrap();
    malformed.save(TableOptions::new(&output)).unwrap();
    let before = snapshot(&output);
    let error = solve(&ms, gain(&output), CalibrationOutputMode::Append).unwrap_err();
    assert!(error.to_string().contains("missing CPARAM"), "{error}");
    assert_eq!(snapshot(&output), before);
}

#[test]
fn restarted_process_refuses_another_process_backup_and_reservation() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let output = dir.path().join("g.cal");
    solve(&ms, gain(&output), CalibrationOutputMode::Replace).unwrap();
    let backup = dir.path().join(".g.cal.casa-rs-replaced-previous-process");
    std::fs::rename(&output, &backup).unwrap();
    let before = snapshot(&backup);
    // Simulate termination after the old product was moved aside. A fresh
    // process must refuse even when an older writer left no reservation.
    for reserved in [false, true] {
        let reservation = dir.path().join(".g.cal.casa-rs-reservation");
        if reserved {
            std::fs::create_dir(&reservation).unwrap();
        }
        let result = std::process::Command::new(env!("CARGO_BIN_EXE_calibrate"))
            .args(["gencal", "--vis"])
            .arg(&ms)
            .arg("--caltable")
            .arg(&output)
            .args(["--caltype", "opac", "--parameter", "0.1", "--no-save-last"])
            .output()
            .unwrap();
        assert!(!result.status.success());
        assert!(String::from_utf8_lossy(&result.stderr).contains("interrupted"));
        assert!(!output.exists());
        assert_eq!(snapshot(&backup), before);
        assert_eq!(reservation.exists(), reserved);
    }
}

#[test]
fn reports_cannot_overwrite_prior_tables_or_gaincurve_sources() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let prior = dir.path().join("prior.cal");
    solve(&ms, gain(&prior), CalibrationOutputMode::Replace).unwrap();
    let output = dir.path().join("new.cal");
    let before = snapshot(&prior);
    let ms_before = snapshot(&ms);
    for surface in ["gaincal", "bandpass", "applycal", "gencal"] {
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_calibrate"));
        command.args([surface, "--vis"]).arg(&ms);
        if matches!(surface, "gaincal" | "bandpass") {
            command.args(["--refant", "0"]);
        }
        if surface != "applycal" {
            command.arg("--caltable").arg(&output);
        }
        if surface == "gencal" {
            command
                .args(["--caltype", "gceff", "--gaincurve-table"])
                .arg(&prior);
        } else {
            command.arg("--gaintable").arg(&prior);
        }
        let result = command
            .args(["--overwrite", "--no-save-last", "--output"])
            .arg(prior.join("table.dat"))
            .output()
            .unwrap();
        assert!(!result.status.success());
        assert!(
            String::from_utf8_lossy(&result.stderr).contains("overlaps"),
            "{surface}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(!output.exists());
        assert_eq!(snapshot(&prior), before);
        assert_eq!(snapshot(&ms), ms_before);
    }
}

#[cfg(unix)]
#[test]
fn export_symlink_parent_is_resolved_before_parent_components() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let alias = dir.path().join("alias");
    std::os::unix::fs::symlink(ms.join("ANTENNA"), &alias).unwrap();
    let before = snapshot(&ms);
    // The library takes filesystem paths directly; CLI path normalization
    // otherwise folds the parent component before the provider sees it.
    let result = export_corrected_data(&ExportCorrectedDataRequest {
        input_ms: ms.clone(),
        output_ms: alias.join("../nested-output.ms"),
        selection: MsSelection::new(),
    });
    assert!(matches!(
        result,
        Err(ExportCorrectedDataError::SameInputOutput { .. })
    ));
    assert_eq!(snapshot(&ms), before);
}

#[test]
fn append_preserves_custom_keyword_subtables() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let output = dir.path().join("g.cal");
    solve(&ms, gain(&output), CalibrationOutputMode::Replace).unwrap();
    let provenance = output.join("PROVENANCE");
    let mut extra = Table::with_schema(
        casa_tables::TableSchema::new(vec![casa_tables::ColumnSchema::scalar(
            "ORIGIN",
            casa_core::PrimitiveType::Float64,
        )])
        .unwrap(),
    );
    extra
        .add_row(RecordValue::new(vec![casa_core::RecordField::new(
            "ORIGIN",
            Value::Scalar(ScalarValue::Float64(42.0)),
        )]))
        .unwrap();
    extra.save(TableOptions::new(&provenance)).unwrap();
    let mut main = table(&output);
    main.keywords_mut()
        .upsert("PROVENANCE", Value::table_ref("PROVENANCE"));
    main.save(TableOptions::new(&output)).unwrap();
    let before = rows(&provenance);
    let row_count = main.row_count();
    drop(main);
    solve(&ms, gain(&output), CalibrationOutputMode::Append).unwrap();
    assert_eq!(table(&output).row_count(), row_count * 2);
    assert_eq!(
        table(&output).keywords().get("PROVENANCE"),
        Some(&Value::table_ref("PROVENANCE"))
    );
    assert_eq!(rows(&provenance), before);
}
