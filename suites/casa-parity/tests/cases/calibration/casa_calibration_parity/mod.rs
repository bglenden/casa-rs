// SPDX-License-Identifier: LGPL-3.0-or-later

use super::common;

use casa_core::{ArrayValue, Complex32};
use casa_ms::MeasurementSet;
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::selection::{MsSelection, MsSelectionIoBudget, ResolvedMsSelectionRow};
use casa_tables::{Table, TableOptions};
use ndarray::Ix2;
use tempfile::TempDir;

use casa_calibration::{
    apply::{
        ApplyCalibrationTableSpec, ApplyMode, ApplyPlanRequest, GainFieldSelector,
        execute_apply_from_path,
    },
    bandpass::{BandpassSolveCombine, BandpassSolveReport, BandpassSolveRequest, BandpassType},
    callib::load_apply_specs_from_callib,
    fluxscale::{FluxScaleRequest, fluxscale},
    solve::{
        CalibrationDataset, CalibrationError, CalibrationSolveRequest, CalibrationSolveResult,
        GainSolveCombine, GainSolveInterval, GainSolveMode, GainSolveModelSource, GainSolveReport,
        GainSolveRequest, GainType, RefAntSelector, solve_calibration,
    },
    table::{CalibrationParameterFamily, summarize_table},
};

fn run_gain_solve(
    path: impl AsRef<std::path::Path>,
    request: &GainSolveRequest,
) -> Result<GainSolveReport, CalibrationError> {
    match solve_calibration(
        CalibrationDataset::path(path),
        CalibrationSolveRequest::Gain(request.clone()),
    )? {
        CalibrationSolveResult::Gain(report) => Ok(report),
        CalibrationSolveResult::Bandpass(_) | CalibrationSolveResult::LegacyEp { .. } => {
            unreachable!("gain request result")
        }
    }
}

fn run_bandpass_solve(
    path: impl AsRef<std::path::Path>,
    request: &BandpassSolveRequest,
) -> Result<BandpassSolveReport, CalibrationError> {
    match solve_calibration(
        CalibrationDataset::path(path),
        CalibrationSolveRequest::Bandpass(request.clone()),
    )? {
        CalibrationSolveResult::Bandpass(report) => Ok(report),
        CalibrationSolveResult::Gain(_) | CalibrationSolveResult::LegacyEp { .. } => {
            unreachable!("bandpass request result")
        }
    }
}

#[test]
fn summarize_casa_generated_gain_t_and_bandpass_tables() {
    let dir = TempDir::new().expect("tempdir");
    let (g_path, t_path, b_path) =
        common::generate_casa_exemplars(dir.path()).unwrap_or_else(|reason| panic!("{reason}"));

    let g_summary = summarize_table(&g_path).expect("summarize G");
    let t_summary = summarize_table(&t_path).expect("summarize T");
    let b_summary = summarize_table(&b_path).expect("summarize B");

    for summary in [&g_summary, &t_summary, &b_summary] {
        assert_eq!(summary.table_type, "Calibration");
        assert_eq!(
            summary.parameter_family,
            CalibrationParameterFamily::Complex
        );
        assert!(summary.row_count > 0);
        assert!(
            summary.columns.iter().any(|column| column == "CPARAM"),
            "missing CPARAM in {}",
            summary.path.display()
        );
    }

    assert_eq!(g_summary.table_subtype, "G Jones");
    assert_eq!(t_summary.table_subtype, "T Jones");
    assert_eq!(b_summary.table_subtype, "B Jones");
}

#[test]
fn apply_combined_scan_field_gain_table_matches_casa_applycal_on_ngc5921_subset() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = common::staged_ngc5921(dir.path()).expect("stage ngc5921");
    let casa_gcal = common::run_casa_gaincal(
        dir.path(),
        &source_ms,
        common::CasaGaincalOptions {
            field: "0,1",
            spw: "0",
            refant: "VA15",
            gaintype: "G",
            calmode: "p",
            solint: "inf",
            combine: "scan,field",
            prior_gaintables: vec![],
            parang: false,
        },
    )
    .unwrap_or_else(|reason| panic!("{reason}"));

    let rust_apply_ms = dir.path().join("rust-apply-combine-scan-field.ms");
    let casa_apply_ms = dir.path().join("casa-apply-combine-scan-field.ms");
    common::copy_measurement_set(&source_ms, &rust_apply_ms).expect("copy Rust apply MS");
    common::copy_measurement_set(&source_ms, &casa_apply_ms).expect("copy CASA apply MS");

    common::run_casa_applycal(
        &casa_apply_ms,
        &casa_gcal,
        "0,1",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply CASA combined-field gain table");
    execute_apply_from_path(
        &rust_apply_ms,
        &ApplyPlanRequest {
            selection: MsSelection::new().field(&[0, 1]).spw(&[0]),
            apply_mode: ApplyMode::CalFlag,
            parang: false,
            calibration_tables: vec![ApplyCalibrationTableSpec::new(&casa_gcal)],
        },
    )
    .expect("apply combined-field CASA gain table in Rust");

    assert_apply_state_close_with_tolerance(
        &rust_apply_ms,
        &casa_apply_ms,
        &MsSelection::new().field(&[0, 1]).spw(&[0]),
        5.0e-4,
    );
}

#[test]
fn solve_gain_phase_g_combine_scan_and_field_writes_one_solution_group_across_fields() {
    let dir = TempDir::new().expect("tempdir");

    let gains = [
        [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
        [
            Complex32::new(0.9553365, 0.29552022),
            Complex32::new(0.921061, -0.38941833),
        ],
        [
            Complex32::new(0.9800666, -0.19866933),
            Complex32::new(0.87758255, 0.47942555),
        ],
    ];
    let source_ms = common::create_gain_solve_fixture_ms_from_clusters(
        dir.path(),
        &[common::synthetic_tables::SyntheticGainTimeCluster {
            time_seconds: 100.0,
            scan_number: 1,
            gains,
        }],
    );
    let mut fixture_ms = MeasurementSet::open(&source_ms).expect("reopen synthetic MS");
    common::append_gain_solve_cluster_for_field(
        &mut fixture_ms,
        1,
        &common::synthetic_tables::SyntheticGainTimeCluster {
            time_seconds: 200.0,
            scan_number: 2,
            gains,
        },
    );
    fixture_ms.save().expect("save synthetic multifield MS");

    let rust_gcal = dir.path().join("rust-combine-scan-field.gcal");
    let report = run_gain_solve(
        &source_ms,
        &GainSolveRequest {
            selection: MsSelection::new().field(&[0, 1]).spw(&[0]),
            output_table: rust_gcal.clone(),
            gain_type: GainType::G,
            solve_mode: GainSolveMode::Phase,
            solve_interval: GainSolveInterval::Infinite,
            combine: GainSolveCombine {
                scans: true,
                fields: true,
                ..GainSolveCombine::default()
            },
            refant: RefAntSelector::AntennaId(0),
            prior_calibration_tables: Vec::new(),
            parang: false,
            model_source: GainSolveModelSource::PointSource,
            normalize_average_amplitude: false,
            preaverage_seconds: 0.0,
            correlation_dependent_flags: false,
            normalization_type: Default::default(),
            robustness: Default::default(),
            min_snr: 0.0,
            // Each of the fixture's three antennas has two baselines; CASA's
            // default minblperant=4 would fail every solve, as CASA's does.
            min_baselines_per_antenna: 2,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("run Rust combine-scan-field solve");
    assert_eq!(report.solution_row_count, 3);
}

#[test]
fn fluxscale_matches_casa_on_ngc5921_gain_table() {
    let dir = TempDir::new().expect("tempdir");
    let (fixture_ms, input_gcal) = common::generate_casa_fluxscale_gain_fixture(
        dir.path(),
        "1331+305*",
        "1445+099*",
        "0",
        "VA15",
    )
    .unwrap_or_else(|reason| panic!("{reason}"));
    let casa_fcal = dir.path().join("casa.fcal");
    let casa_report = common::run_casa_fluxscale(
        &fixture_ms,
        &input_gcal,
        &casa_fcal,
        "1331+305*",
        "1445+099*",
    )
    .unwrap_or_else(|reason| panic!("{reason}"));

    let rust_fcal = dir.path().join("rust.fcal");
    let rust_report = fluxscale(&FluxScaleRequest {
        options: Default::default(),
        append: false,
        input_table: input_gcal.clone(),
        output_table: rust_fcal.clone(),
        reference_fields: vec!["1331+305*".to_string()],
        transfer_fields: vec!["1445+099*".to_string()],
        refspwmap: Vec::new(),
        gainthreshold: None,
        incremental: false,
    })
    .expect("run Rust fluxscale");

    let casa_field = find_casa_fluxscale_field(&casa_report, "1445+099");
    let casa_fit_fluxd = casa_fit_fluxd(casa_field);
    let casa_fit_ref_frequency_hz = casa_fit_ref_frequency_hz(casa_field);
    let rust_field = rust_report
        .fields
        .values()
        .next()
        .expect("Rust transfer field");

    assert!(
        (rust_field.fit_fluxd - casa_fit_fluxd).abs() <= 5.0e-3,
        "fit flux mismatch: casa={casa_fit_fluxd:.9} rust={:.9}",
        rust_field.fit_fluxd
    );
    assert!(
        (rust_field.fit_ref_frequency_hz - casa_fit_ref_frequency_hz).abs() <= 1.0,
        "fit ref frequency mismatch: casa={casa_fit_ref_frequency_hz:.6} rust={:.6}",
        rust_field.fit_ref_frequency_hz
    );

    let rust_rows = read_cparam_rows_for_field(&rust_fcal, 1);
    let casa_rows = read_cparam_rows_for_field(&casa_fcal, 1);
    assert_eq!(
        rust_rows.len(),
        casa_rows.len(),
        "transfer row count mismatch"
    );
    for (row_index, (rust_row, casa_row)) in rust_rows.iter().zip(casa_rows.iter()).enumerate() {
        assert_eq!(
            rust_row.len(),
            casa_row.len(),
            "transfer gain count mismatch on row {row_index}"
        );
        for (sample_index, (rust_gain, casa_gain)) in
            rust_row.iter().zip(casa_row.iter()).enumerate()
        {
            let re_diff = (rust_gain.re - casa_gain.re).abs();
            let im_diff = (rust_gain.im - casa_gain.im).abs();
            assert!(
                re_diff <= 5.0e-3 && im_diff <= 5.0e-3,
                "CPARAM mismatch on row {row_index} sample {sample_index}: casa=({:.6},{:.6}) rust=({:.6},{:.6})",
                casa_gain.re,
                casa_gain.im,
                rust_gain.re,
                rust_gain.im
            );
        }
    }
}

#[test]
fn solve_t_phase_gain_with_prior_g_preapply_matches_casa_gaincal_downstream() {
    let dir = TempDir::new().expect("tempdir");
    let source_ms = common::staged_ngc5921(dir.path()).expect("stage ngc5921");
    let prior_g = common::run_casa_gaincal(
        dir.path(),
        &source_ms,
        common::CasaGaincalOptions {
            field: "0",
            spw: "0",
            refant: "VA15",
            gaintype: "G",
            calmode: "p",
            solint: "inf",
            combine: "",
            prior_gaintables: vec![],
            parang: false,
        },
    )
    .unwrap_or_else(|reason| panic!("{reason}"));
    let casa_t = common::run_casa_gaincal(
        dir.path(),
        &source_ms,
        common::CasaGaincalOptions {
            field: "0",
            spw: "0",
            refant: "VA15",
            gaintype: "T",
            calmode: "p",
            solint: "int",
            combine: "",
            prior_gaintables: vec![prior_g.as_path()],
            parang: false,
        },
    )
    .unwrap_or_else(|reason| panic!("{reason}"));
    let rust_t = dir.path().join("rust-prior-t.gcal");
    run_gain_solve(
        &source_ms,
        &GainSolveRequest {
            selection: MsSelection::new().field(&[0]).spw(&[0]),
            output_table: rust_t.clone(),
            gain_type: GainType::T,
            solve_mode: GainSolveMode::Phase,
            solve_interval: GainSolveInterval::Integration,
            combine: GainSolveCombine::default(),
            refant: RefAntSelector::AntennaName("VA15".to_string()),
            prior_calibration_tables: vec![ApplyCalibrationTableSpec::new(&prior_g)],
            parang: false,
            model_source: GainSolveModelSource::PointSource,
            normalize_average_amplitude: false,
            preaverage_seconds: 0.0,
            correlation_dependent_flags: false,
            normalization_type: Default::default(),
            robustness: Default::default(),
            min_snr: 0.0,
            min_baselines_per_antenna: 4,
            smodel: [1.0, 0.0, 0.0, 0.0],
        },
    )
    .expect("run Rust T solve with prior preapply");

    let casa_apply_ms = dir.path().join("casa-solved-prior-t.ms");
    let rust_apply_ms = dir.path().join("rust-solved-prior-t.ms");
    common::copy_measurement_set(&source_ms, &casa_apply_ms).expect("copy casa apply ms");
    common::copy_measurement_set(&source_ms, &rust_apply_ms).expect("copy rust apply ms");

    common::run_casa_applycal_chain(
        &casa_apply_ms,
        &[prior_g.as_path(), casa_t.as_path()],
        "0",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply CASA prior+T chain");
    common::run_casa_applycal_chain(
        &rust_apply_ms,
        &[prior_g.as_path(), rust_t.as_path()],
        "0",
        "0",
        None,
        "calflag",
        false,
        false,
    )
    .expect("apply Rust prior+T chain");

    assert_apply_state_close_with_tolerance(
        &rust_apply_ms,
        &casa_apply_ms,
        &MsSelection::new().field(&[0]).spw(&[0]),
        1.0e-3,
    );
}

fn assert_apply_state_close(
    left_path: &std::path::Path,
    right_path: &std::path::Path,
    selection: &MsSelection,
) {
    assert_apply_state_close_with_complex_tolerances(left_path, right_path, selection, 5.0e-4, 0.0);
}

fn assert_apply_state_close_with_tolerance(
    left_path: &std::path::Path,
    right_path: &std::path::Path,
    selection: &MsSelection,
    complex_tolerance: f32,
) {
    assert_apply_state_close_with_complex_tolerances(
        left_path,
        right_path,
        selection,
        complex_tolerance,
        0.0,
    );
}

fn assert_apply_state_close_with_complex_tolerances(
    left_path: &std::path::Path,
    right_path: &std::path::Path,
    selection: &MsSelection,
    complex_abs_tolerance: f32,
    complex_rel_tolerance: f32,
) {
    let left = MeasurementSet::open(left_path).expect("open left MeasurementSet");
    let right = MeasurementSet::open(right_path).expect("open right MeasurementSet");

    let selection_budget = MsSelectionIoBudget {
        available_bytes: 1024 * 1024,
        maximum_live_blocks: 2,
        requested_bytes_per_row: std::mem::size_of::<ResolvedMsSelectionRow>(),
        storage_alignment_rows: None,
    };
    let left_rows = left
        .resolve_selection(selection, selection_budget)
        .expect("select left rows")
        .row_indices()
        .collect::<Vec<_>>();
    let right_rows = right
        .resolve_selection(selection, selection_budget)
        .expect("select right rows")
        .row_indices()
        .collect::<Vec<_>>();
    assert_eq!(left_rows, right_rows, "selected row mismatch");
    assert!(!left_rows.is_empty(), "selected rows should not be empty");

    let left_corrected = left
        .data_column(VisibilityDataColumn::CorrectedData)
        .expect("left CORRECTED_DATA");
    let right_corrected = right
        .data_column(VisibilityDataColumn::CorrectedData)
        .expect("right CORRECTED_DATA");

    for row in left_rows {
        let left_data = left_corrected.get(row).expect("left corrected row");
        let right_data = right_corrected.get(row).expect("right corrected row");
        assert_complex_cells_close(
            row,
            left_data,
            right_data,
            complex_abs_tolerance,
            complex_rel_tolerance,
        );

        assert_eq!(
            left.flag_row_column().get(row).expect("left flag_row"),
            right.flag_row_column().get(row).expect("right flag_row"),
            "FLAG_ROW mismatch on row {row}"
        );

        let left_flags = flag_matrix_for_row(&left, row);
        let right_flags = flag_matrix_for_row(&right, row);
        assert_eq!(left_flags, right_flags, "FLAG mismatch on row {row}");

        let left_weight = left
            .main_table()
            .cell_accessor(row, "WEIGHT")
            .and_then(|cell| cell.array())
            .expect("left WEIGHT");
        let right_weight = right
            .main_table()
            .cell_accessor(row, "WEIGHT")
            .and_then(|cell| cell.array())
            .expect("right WEIGHT");
        assert_float_cells_close(row, "WEIGHT", left_weight, right_weight, 5.0e-4);

        let left_has_weight_spectrum = left
            .main_table()
            .schema()
            .is_some_and(|schema| schema.contains_column("WEIGHT_SPECTRUM"));
        let right_has_weight_spectrum = right
            .main_table()
            .schema()
            .is_some_and(|schema| schema.contains_column("WEIGHT_SPECTRUM"));
        assert_eq!(
            left_has_weight_spectrum, right_has_weight_spectrum,
            "WEIGHT_SPECTRUM presence mismatch on row {row}"
        );
        if left_has_weight_spectrum {
            let left_weight_spectrum = left
                .main_table()
                .cell_accessor(row, "WEIGHT_SPECTRUM")
                .and_then(|cell| cell.array())
                .expect("left WEIGHT_SPECTRUM");
            let right_weight_spectrum = right
                .main_table()
                .cell_accessor(row, "WEIGHT_SPECTRUM")
                .and_then(|cell| cell.array())
                .expect("right WEIGHT_SPECTRUM");
            assert_float_cells_close(
                row,
                "WEIGHT_SPECTRUM",
                left_weight_spectrum,
                right_weight_spectrum,
                5.0e-4,
            );
        }
    }
}

fn assert_complex_cells_close(
    row: usize,
    left: &ArrayValue,
    right: &ArrayValue,
    abs_tolerance: f32,
    rel_tolerance: f32,
) {
    let ArrayValue::Complex32(left) = left else {
        panic!("left CORRECTED_DATA row {row} was not Complex32");
    };
    let ArrayValue::Complex32(right) = right else {
        panic!("right CORRECTED_DATA row {row} was not Complex32");
    };

    assert_eq!(
        left.shape(),
        right.shape(),
        "CORRECTED_DATA shape mismatch on row {row}"
    );

    for (index, (left_value, right_value)) in left.iter().zip(right.iter()).enumerate() {
        assert_complex_close(
            row,
            index,
            *left_value,
            *right_value,
            abs_tolerance,
            rel_tolerance,
        );
    }
}

fn assert_complex_close(
    row: usize,
    index: usize,
    left: Complex32,
    right: Complex32,
    abs_tolerance: f32,
    rel_tolerance: f32,
) {
    let re_diff = (left.re - right.re).abs();
    let im_diff = (left.im - right.im).abs();
    let re_limit = abs_tolerance.max(rel_tolerance * left.re.abs().max(right.re.abs()));
    let im_limit = abs_tolerance.max(rel_tolerance * left.im.abs().max(right.im.abs()));
    assert!(
        re_diff <= re_limit && im_diff <= im_limit,
        "CORRECTED_DATA mismatch on row {row} sample {index}: left=({:.6},{:.6}) right=({:.6},{:.6}) abs_tolerance={abs_tolerance} rel_tolerance={rel_tolerance}",
        left.re,
        left.im,
        right.re,
        right.im
    );
}

fn assert_float_cells_close(
    row: usize,
    column: &str,
    left: &ArrayValue,
    right: &ArrayValue,
    tolerance: f32,
) {
    let ArrayValue::Float32(left) = left else {
        panic!("left {column} row {row} was not Float32");
    };
    let ArrayValue::Float32(right) = right else {
        panic!("right {column} row {row} was not Float32");
    };

    assert_eq!(
        left.shape(),
        right.shape(),
        "{column} shape mismatch on row {row}"
    );

    for (index, (left_value, right_value)) in left.iter().zip(right.iter()).enumerate() {
        let diff = (left_value - right_value).abs();
        assert!(
            diff <= tolerance,
            "{column} mismatch on row {row} sample {index}: left={left_value:.6} right={right_value:.6} tolerance={tolerance}"
        );
    }
}

fn flag_matrix_for_row(ms: &MeasurementSet, row: usize) -> ndarray::Array2<bool> {
    match ms.flag_column().get(row).expect("flag cell") {
        ArrayValue::Bool(values) => values
            .view()
            .into_dimensionality::<Ix2>()
            .expect("2d flag cell")
            .to_owned(),
        other => panic!("unexpected FLAG cell type {:?}", other.primitive_type()),
    }
}

fn read_cparam_rows_for_field(table_path: &std::path::Path, field_id: i32) -> Vec<Vec<Complex32>> {
    let table = Table::open(TableOptions::new(table_path)).expect("open caltable");
    let mut rows = Vec::new();
    for row in 0..table.row_count() {
        let current_field_id = match table
            .cell_accessor(row, "FIELD_ID")
            .and_then(|cell| cell.scalar())
            .expect("FIELD_ID cell")
        {
            casa_core::ScalarValue::Int32(value) => *value,
            other => panic!("unexpected FIELD_ID value: {other:?}"),
        };
        if current_field_id != field_id {
            continue;
        }
        let gains = match table
            .cell_accessor(row, "CPARAM")
            .and_then(|cell| cell.array())
            .expect("CPARAM cell")
        {
            ArrayValue::Complex32(values) => values.iter().copied().collect::<Vec<_>>(),
            other => panic!("unexpected CPARAM value: {other:?}"),
        };
        rows.push(gains);
    }
    rows
}

fn read_field_direction(table_path: &std::path::Path, field_id: usize) -> (f64, f64) {
    let table = Table::open(TableOptions::new(table_path.join("FIELD"))).expect("open FIELD table");
    let phase_dir = match table
        .cell_accessor(field_id, "PHASE_DIR")
        .and_then(|cell| cell.array())
        .expect("PHASE_DIR cell")
    {
        ArrayValue::Float64(values) => values,
        other => panic!("unexpected PHASE_DIR value: {other:?}"),
    };
    (phase_dir[[0, 0]], phase_dir[[1, 0]])
}

fn find_casa_fluxscale_field<'a>(
    report: &'a serde_json::Value,
    name_fragment: &str,
) -> &'a serde_json::Value {
    let object = report.as_object().expect("CASA fluxscale object");
    object
        .values()
        .find(|value| {
            value
                .get("fieldName")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|name| name.contains(name_fragment))
        })
        .unwrap_or_else(|| panic!("missing CASA fluxscale field containing {name_fragment}"))
}

fn casa_fit_fluxd(field: &serde_json::Value) -> f64 {
    match field.get("fitFluxd").expect("fitFluxd") {
        serde_json::Value::Array(values) => values
            .first()
            .and_then(serde_json::Value::as_f64)
            .expect("fitFluxd[0]"),
        serde_json::Value::Number(value) => value.as_f64().expect("fitFluxd number"),
        other => panic!("unexpected fitFluxd value: {other:?}"),
    }
}

fn casa_fit_ref_frequency_hz(field: &serde_json::Value) -> f64 {
    field
        .get("fitRefFreq")
        .and_then(serde_json::Value::as_f64)
        .expect("fitRefFreq")
}

mod apply;
mod bandpass_solve;
mod gain_solve;
