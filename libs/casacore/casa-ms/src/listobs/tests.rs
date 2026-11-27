// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

use casa_core::{RecordField, RecordValue, ScalarValue, Value};
use ndarray::ArrayD;

use crate::MeasurementSetBuilder;
use crate::test_helpers::default_value;

#[test]
fn summary_collects_core_sections() {
    let ms = make_summary_fixture();

    let summary = ListObsSummary::from_ms(&ms).expect("build summary");
    assert_eq!(summary.schema_version, 1);
    assert_eq!(summary.measurement_set.row_count, 2);
    assert_eq!(summary.observations.len(), 1);
    assert_eq!(summary.fields.len(), 1);
    assert_eq!(summary.spectral_windows.len(), 1);
    assert_eq!(summary.sources.len(), 1);
    assert_eq!(summary.antennas.len(), 2);
    assert_eq!(summary.scans.len(), 1);
    assert_eq!(summary.scans[0].spectral_window_ids, vec![0]);
    assert_eq!(summary.scans[0].field_names, vec!["3C286".to_string()]);
    assert_eq!(
        summary.measurement_set.time_reference.as_deref(),
        Some("UTC")
    );
}

#[test]
fn scan_summary_merges_state_ids_like_casa_listobs() {
    let mut ms = MeasurementSet::create_memory(
        MeasurementSetBuilder::new().with_optional_subtable(crate::SubtableId::Source),
    )
    .expect("create MS");
    add_observation_row(&mut ms, 4_981_000_000.0, 4_981_000_030.0);
    add_field_row(&mut ms);
    add_source_row(&mut ms);
    add_state_row(&mut ms, "CALIBRATE_PHASE.ON_SOURCE");
    add_state_row(&mut ms, "CALIBRATE_WVR.ON_SOURCE");
    add_spectral_window_row(&mut ms);
    add_polarization_row(&mut ms);
    add_data_description_row(&mut ms);
    add_antenna_rows(&mut ms);
    add_main_row_with_selection(&mut ms, 4_981_000_000.0, 0, 1, 0, 0, [0.0, 0.0, 0.0]);
    add_main_row_with_selection(&mut ms, 4_981_000_015.0, 1, 1, 0, 1, [0.0, 0.0, 0.0]);

    let summary = ListObsSummary::from_ms(&ms).expect("build summary");

    assert_eq!(summary.scans.len(), 1);
    assert_eq!(summary.scans[0].row_count, 2);
    assert_eq!(summary.scans[0].state_ids, vec![0, 1]);
    assert_eq!(
        summary.scans[0].scan_intents,
        vec![
            "CALIBRATE_PHASE.ON_SOURCE".to_string(),
            "CALIBRATE_WVR.ON_SOURCE".to_string()
        ]
    );
}

#[test]
fn text_renderer_mentions_casa_sections() {
    let summary = ListObsSummary::from_ms(&make_summary_fixture()).expect("build summary");
    let text = summary.render_text();

    assert!(text.contains("MeasurementSet Name:"));
    assert!(text.contains("ObservationID = 0"));
    assert!(text.contains("Date        Timerange"));
    assert!(text.contains("Fields: 1"));
    assert!(text.contains(
        "Spectral Windows: (1 unique spectral windows and 1 unique polarization setups)"
    ));
    assert!(text.contains("Sources: 1"));
    assert!(text.contains("Antennas: 2:"));
    assert!(text.contains("3C286"));
}

#[test]
fn terse_renderer_omits_verbose_only_sections() {
    let summary = ListObsSummary::from_ms(&make_summary_fixture()).expect("build summary");
    let text = summary.render_text_with_options(&ListObsOptions {
        verbose: false,
        ..ListObsOptions::default()
    });

    assert!(text.contains("Observation: VLA(2 antennas)"));
    assert!(text.contains("Antennas: 2 'name'='station'"));
    assert!(!text.contains("Sources:"));
    assert!(!text.contains("ObservationID ="));
}

#[test]
fn correlation_selection_filters_matching_data_descriptions() {
    let ms = make_selection_fixture();
    let summary = ListObsSummary::from_ms_with_options(
        &ms,
        &ListObsOptions {
            correlation: Some("XX".to_string()),
            ..ListObsOptions::default()
        },
    )
    .expect("build selected summary");

    assert_eq!(summary.measurement_set.row_count, 2);
    assert_eq!(summary.data_descriptions.len(), 1);
    assert_eq!(summary.data_descriptions[0].data_description_id, 0);
    assert_eq!(summary.scans.len(), 2);
}

#[test]
fn intent_selection_filters_matching_state_rows() {
    let ms = make_selection_fixture();
    let summary = ListObsSummary::from_ms_with_options(
        &ms,
        &ListObsOptions {
            intent: Some("CALIBRATE*".to_string()),
            ..ListObsOptions::default()
        },
    )
    .expect("build selected summary");

    assert_eq!(summary.measurement_set.row_count, 1);
    assert_eq!(summary.scans.len(), 1);
    assert_eq!(
        summary.scans[0].scan_intents,
        vec!["CALIBRATE_PHASE.ON_SOURCE".to_string()]
    );
}

#[test]
fn uvrange_selection_filters_rows_in_meters() {
    let ms = make_selection_fixture();
    let summary = ListObsSummary::from_ms_with_options(
        &ms,
        &ListObsOptions {
            uvrange: Some("0~100m".to_string()),
            ..ListObsOptions::default()
        },
    )
    .expect("build selected summary");

    assert_eq!(summary.measurement_set.row_count, 2);
    assert_eq!(summary.scans.len(), 2);
}

#[test]
fn uvrange_selection_accepts_casa_kilometer_units() {
    let ms = make_selection_fixture();
    let summary = ListObsSummary::from_ms_with_options(
        &ms,
        &ListObsOptions {
            uvrange: Some("0~0.1km".to_string()),
            ..ListObsOptions::default()
        },
    )
    .expect("build selected summary");

    assert_eq!(summary.measurement_set.row_count, 2);
    assert_eq!(summary.scans.len(), 2);
}

#[test]
fn uvrange_selection_filters_rows_in_lambda_units() {
    let ms = make_selection_fixture();
    let summary = ListObsSummary::from_ms_with_options(
        &ms,
        &ListObsOptions {
            uvrange: Some("0~200lambda".to_string()),
            ..ListObsOptions::default()
        },
    )
    .expect("build selected summary");

    assert_eq!(summary.measurement_set.row_count, 1);
    assert_eq!(summary.scans.len(), 1);
    assert_eq!(summary.scans[0].scan_number, 3);
}

#[test]
fn timerange_selection_filters_rows_by_time_of_day() {
    let ms = make_selection_fixture();
    let summary = ListObsSummary::from_ms_with_options(
        &ms,
        &ListObsOptions {
            timerange: Some("11:06:35~11:06:55".to_string()),
            ..ListObsOptions::default()
        },
    )
    .expect("build selected summary");

    assert_eq!(summary.measurement_set.row_count, 2);
    assert_eq!(summary.scans.len(), 2);
    assert_eq!(summary.scans[0].scan_number, 1);
    assert_eq!(summary.scans[1].scan_number, 2);
}

#[test]
fn timerange_interval_selection_filters_rows() {
    let ms = make_selection_fixture();
    let summary = ListObsSummary::from_ms_with_options(
        &ms,
        &ListObsOptions {
            timerange: Some("2016-09-19T11:06:40+00:00:15".to_string()),
            ..ListObsOptions::default()
        },
    )
    .expect("build selected summary");

    assert_eq!(summary.measurement_set.row_count, 2);
    assert_eq!(summary.scans.len(), 2);
    assert_eq!(summary.scans[0].scan_number, 1);
    assert_eq!(summary.scans[1].scan_number, 2);
}

#[test]
fn selector_rejects_empty_timerange() {
    let ms = make_selection_fixture();

    assert_selection_error_contains(
        &ms,
        ListObsOptions {
            timerange: Some("  ".to_string()),
            ..ListObsOptions::default()
        },
        "timerange selector was empty",
    );
}

#[test]
fn selector_rejects_invalid_timerange_fragments() {
    let ms = make_selection_fixture();

    assert_selection_error_contains(
        &ms,
        ListObsOptions {
            timerange: Some("2016/13/40".to_string()),
            ..ListObsOptions::default()
        },
        "invalid UTC timestamp",
    );
    assert_selection_error_contains(
        &ms,
        ListObsOptions {
            timerange: Some("11:not-a-minute".to_string()),
            ..ListObsOptions::default()
        },
        "not a valid integer",
    );
}

#[test]
fn selector_rejects_mixed_uvrange_units() {
    let ms = make_selection_fixture();

    assert_selection_error_contains(
        &ms,
        ListObsOptions {
            uvrange: Some("1m~2lambda".to_string()),
            ..ListObsOptions::default()
        },
        "uvrange bounds must use the same unit",
    );
}

#[test]
fn selector_rejects_unmatched_field_globs() {
    let ms = make_selection_fixture();

    assert_selection_error_contains(
        &ms,
        ListObsOptions {
            field: Some("NO_SUCH_FIELD*".to_string()),
            ..ListObsOptions::default()
        },
        "did not match any field names",
    );
}

#[test]
fn selector_rejects_unmatched_intents() {
    let ms = make_selection_fixture();

    assert_selection_error_contains(
        &ms,
        ListObsOptions {
            intent: Some("NO_SUCH_INTENT*".to_string()),
            ..ListObsOptions::default()
        },
        "did not match any STATE rows",
    );
}

#[test]
fn correlation_selector_rejects_empty_and_invalid_values() {
    let empty = parse_correlation_selector("").expect_err("empty correlation should fail");
    assert!(
        format!("{empty}").contains("correlation selector was empty"),
        "{empty}"
    );

    let invalid =
        parse_correlation_selector("RR,NOTACORR").expect_err("invalid correlation should fail");
    assert!(
        format!("{invalid}").contains("not a supported Stokes/correlation name"),
        "{invalid}"
    );
}

#[test]
fn listunfl_reports_fractional_unflagged_counts() {
    let ms = make_selection_fixture();
    let summary = ListObsSummary::from_ms_with_options(
        &ms,
        &ListObsOptions {
            listunfl: true,
            ..ListObsOptions::default()
        },
    )
    .expect("build listunfl summary");

    assert_eq!(summary.scans[0].unflagged_row_count, Some(0.75));
    assert_eq!(summary.scans[1].unflagged_row_count, Some(0.0));
    assert_eq!(summary.scans[2].unflagged_row_count, Some(1.0));
    let text = summary.render_text();
    assert!(text.contains("nUnflRows"));
    assert!(text.contains("0.75"));
}

#[test]
fn uv_coverage_groups_tracks_and_scales_by_spw_center_frequency() {
    let ms = make_selection_fixture();
    let coverage = ListObsUvCoverage::from_ms(&ms).expect("build uv coverage");

    assert_eq!(coverage.schema_version, LISTOBS_UV_SCHEMA_VERSION);
    assert_eq!(coverage.axis_unit, "lambda");
    assert!(coverage.mirrored_display);
    assert_eq!(coverage.sample_count, 3);
    assert_eq!(coverage.tracks.len(), 2);

    let spw0 = coverage
        .tracks
        .iter()
        .find(|track| track.spectral_window_id == 0)
        .expect("spw0 track");
    assert_eq!(spw0.samples.len(), 2);
    assert_eq!(spw0.samples[0].row, 0);
    assert_eq!(spw0.samples[1].row, 2);

    let spw1 = coverage
        .tracks
        .iter()
        .find(|track| track.spectral_window_id == 1)
        .expect("spw1 track");
    assert_eq!(spw1.samples.len(), 1);
    assert!(spw1.center_frequency_hz > spw0.center_frequency_hz);
    assert!(spw1.samples[0].u_lambda > spw0.samples[0].u_lambda);
    assert!(coverage.max_abs_uv_lambda >= spw1.samples[0].v_lambda.abs());
}

#[test]
fn uv_coverage_respects_uvrange_selection() {
    let ms = make_selection_fixture();
    let coverage = ListObsUvCoverage::from_ms_with_options(
        &ms,
        &ListObsOptions {
            uvrange: Some("0~50m".to_string()),
            ..ListObsOptions::default()
        },
    )
    .expect("build filtered uv coverage");

    assert_eq!(coverage.sample_count, 2);
    assert_eq!(coverage.tracks.len(), 1);
    assert!(
        coverage.tracks[0]
            .samples
            .iter()
            .all(|sample| sample.row != 1)
    );
}

fn assert_selection_error_contains(ms: &MeasurementSet, options: ListObsOptions, expected: &str) {
    let err = selection_from_options(
        ms,
        &options,
        crate::selection::CompiledMsSelection::new(),
        crate::selection::RowGrammar::Standard,
        None,
    )
    .expect_err("selector should fail");
    assert!(
        format!("{err}").contains(expected),
        "expected {expected:?} in {err}"
    );
}

fn make_summary_fixture() -> MeasurementSet {
    let mut ms = MeasurementSet::create_memory(
        MeasurementSetBuilder::new().with_optional_subtable(crate::SubtableId::Source),
    )
    .expect("create MS");
    add_observation_row(&mut ms, 4_981_000_000.0, 4_981_000_030.0);
    add_field_row(&mut ms);
    add_source_row(&mut ms);
    add_spectral_window_row(&mut ms);
    add_polarization_row(&mut ms);
    add_data_description_row(&mut ms);
    add_antenna_rows(&mut ms);
    add_main_row(&mut ms, 4_981_000_000.0, 0, 1);
    add_main_row(&mut ms, 4_981_000_015.0, 1, 1);
    ms
}

fn make_selection_fixture() -> MeasurementSet {
    let mut ms = MeasurementSet::create_memory(
        MeasurementSetBuilder::new().with_optional_subtable(crate::SubtableId::Source),
    )
    .expect("create MS");
    add_observation_row(&mut ms, 4_981_000_000.0, 4_981_000_030.0);
    add_field_row(&mut ms);
    add_source_row(&mut ms);
    add_state_row(&mut ms, "CALIBRATE_PHASE.ON_SOURCE");
    add_state_row(&mut ms, "TARGET.ON_SOURCE");
    add_spectral_window_row_with_ref(&mut ms, "SPW0", 1.4e9);
    add_spectral_window_row_with_ref(&mut ms, "SPW1", 2.8e9);
    add_polarization_row_with_codes(&mut ms, &[9, 12]);
    add_polarization_row_with_codes(&mut ms, &[5, 8]);
    add_data_description_mapping_row(&mut ms, 0, 0);
    add_data_description_mapping_row(&mut ms, 1, 1);
    add_antenna_rows(&mut ms);
    add_main_row_with_selection(&mut ms, 4_981_000_000.0, 1, 1, 0, 0, [30.0, 40.0, 0.0]);
    add_main_row_with_selection(&mut ms, 4_981_000_010.0, 1, 2, 1, 1, [300.0, 400.0, 0.0]);
    add_main_row_with_selection(&mut ms, 4_981_000_020.0, 1, 3, 0, 1, [5.0, 12.0, 0.0]);
    set_main_row_flag_matrix(
        &mut ms,
        0,
        ArrayD::from_shape_vec(vec![2, 2], vec![false, false, false, true]).unwrap(),
    );
    set_main_row_flag_matrix(
        &mut ms,
        1,
        ArrayD::from_shape_vec(vec![2, 2], vec![true, true, true, true]).unwrap(),
    );
    set_main_row_flag_matrix(
        &mut ms,
        2,
        ArrayD::from_shape_vec(vec![2, 2], vec![false, false, false, false]).unwrap(),
    );
    ms
}

fn add_observation_row(ms: &mut MeasurementSet, start: f64, end: f64) {
    let table = ms
        .subtable_mut(crate::SubtableId::Observation)
        .expect("OBSERVATION table");
    let row = RecordValue::new(vec![
        RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
        RecordField::new(
            "LOG",
            Value::Array(ArrayValue::String(
                ArrayD::from_shape_vec(vec![1], vec!["log".to_string()]).unwrap(),
            )),
        ),
        RecordField::new(
            "OBSERVER",
            Value::Scalar(ScalarValue::String("TESTER".to_string())),
        ),
        RecordField::new(
            "PROJECT",
            Value::Scalar(ScalarValue::String("CASA-RS".to_string())),
        ),
        RecordField::new("RELEASE_DATE", Value::Scalar(ScalarValue::Float64(end))),
        RecordField::new(
            "SCHEDULE",
            Value::Array(ArrayValue::String(
                ArrayD::from_shape_vec(vec![1], vec!["default".to_string()]).unwrap(),
            )),
        ),
        RecordField::new(
            "SCHEDULE_TYPE",
            Value::Scalar(ScalarValue::String("standard".to_string())),
        ),
        RecordField::new(
            "TELESCOPE_NAME",
            Value::Scalar(ScalarValue::String("VLA".to_string())),
        ),
        RecordField::new(
            "TIME_RANGE",
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(vec![2], vec![start, end]).unwrap(),
            )),
        ),
    ]);
    table.add_row(row).unwrap();
}

fn add_field_row(ms: &mut MeasurementSet) {
    let table = ms
        .subtable_mut(crate::SubtableId::Field)
        .expect("FIELD table");
    let direction =
        ArrayValue::Float64(ArrayD::from_shape_vec(vec![2, 1], vec![1.234, 0.456]).unwrap());
    let row = RecordValue::new(vec![
        RecordField::new("CODE", Value::Scalar(ScalarValue::String("C".to_string()))),
        RecordField::new("DELAY_DIR", Value::Array(direction.clone())),
        RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
        RecordField::new(
            "NAME",
            Value::Scalar(ScalarValue::String("3C286".to_string())),
        ),
        RecordField::new("NUM_POLY", Value::Scalar(ScalarValue::Int32(0))),
        RecordField::new("PHASE_DIR", Value::Array(direction.clone())),
        RecordField::new("REFERENCE_DIR", Value::Array(direction)),
        RecordField::new("SOURCE_ID", Value::Scalar(ScalarValue::Int32(0))),
        RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(4_981_000_000.0))),
    ]);
    table.add_row(row).unwrap();
}

fn add_source_row(ms: &mut MeasurementSet) {
    let table = ms
        .subtable_mut(crate::SubtableId::Source)
        .expect("SOURCE table");
    let row = RecordValue::new(vec![
        RecordField::new("CALIBRATION_GROUP", Value::Scalar(ScalarValue::Int32(0))),
        RecordField::new("CODE", Value::Scalar(ScalarValue::String("C".to_string()))),
        RecordField::new(
            "DIRECTION",
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(vec![2], vec![1.234, 0.456]).unwrap(),
            )),
        ),
        RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(30.0))),
        RecordField::new(
            "NAME",
            Value::Scalar(ScalarValue::String("3C286".to_string())),
        ),
        RecordField::new("NUM_LINES", Value::Scalar(ScalarValue::Int32(0))),
        RecordField::new(
            "PROPER_MOTION",
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(vec![2], vec![0.0, 0.0]).unwrap(),
            )),
        ),
        RecordField::new("SOURCE_ID", Value::Scalar(ScalarValue::Int32(0))),
        RecordField::new("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(0))),
        RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(4_981_000_000.0))),
    ]);
    table.add_row(row).unwrap();
}

fn add_spectral_window_row(ms: &mut MeasurementSet) {
    add_spectral_window_row_with_ref(ms, "SPW0", 1.4e9);
}

fn add_spectral_window_row_with_ref(ms: &mut MeasurementSet, name: &str, ref_frequency_hz: f64) {
    let table = ms
        .subtable_mut(crate::SubtableId::SpectralWindow)
        .expect("SPECTRAL_WINDOW table");
    let freq = ArrayValue::Float64(
        ArrayD::from_shape_vec(vec![2], vec![ref_frequency_hz, ref_frequency_hz + 1.0e6]).unwrap(),
    );
    let width = ArrayValue::Float64(ArrayD::from_shape_vec(vec![2], vec![1.0e6, 1.0e6]).unwrap());
    let row = RecordValue::new(vec![
        RecordField::new("CHAN_FREQ", Value::Array(freq)),
        RecordField::new("CHAN_WIDTH", Value::Array(width.clone())),
        RecordField::new("EFFECTIVE_BW", Value::Array(width.clone())),
        RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
        RecordField::new("FREQ_GROUP", Value::Scalar(ScalarValue::Int32(0))),
        RecordField::new(
            "FREQ_GROUP_NAME",
            Value::Scalar(ScalarValue::String("GROUP0".to_string())),
        ),
        RecordField::new("IF_CONV_CHAIN", Value::Scalar(ScalarValue::Int32(0))),
        RecordField::new("MEAS_FREQ_REF", Value::Scalar(ScalarValue::Int32(5))),
        RecordField::new("NAME", Value::Scalar(ScalarValue::String(name.to_string()))),
        RecordField::new("NET_SIDEBAND", Value::Scalar(ScalarValue::Int32(1))),
        RecordField::new("NUM_CHAN", Value::Scalar(ScalarValue::Int32(2))),
        RecordField::new(
            "REF_FREQUENCY",
            Value::Scalar(ScalarValue::Float64(ref_frequency_hz)),
        ),
        RecordField::new("RESOLUTION", Value::Array(width.clone())),
        RecordField::new(
            "TOTAL_BANDWIDTH",
            Value::Scalar(ScalarValue::Float64(2.0e6)),
        ),
    ]);
    table.add_row(row).unwrap();
}

fn add_polarization_row(ms: &mut MeasurementSet) {
    add_polarization_row_with_codes(ms, &[9, 12]);
}

fn add_polarization_row_with_codes(ms: &mut MeasurementSet, corr_types: &[i32]) {
    let table = ms
        .subtable_mut(crate::SubtableId::Polarization)
        .expect("POLARIZATION table");
    let corr_product = match corr_types.len() {
        2 => vec![0, 1, 0, 1],
        4 => vec![0, 0, 1, 1, 0, 1, 0, 1],
        len => vec![0; len * 2],
    };
    let row = RecordValue::new(vec![
        RecordField::new(
            "CORR_PRODUCT",
            Value::Array(ArrayValue::Int32(
                ArrayD::from_shape_vec(vec![2, corr_types.len()], corr_product).unwrap(),
            )),
        ),
        RecordField::new(
            "CORR_TYPE",
            Value::Array(ArrayValue::Int32(
                ArrayD::from_shape_vec(vec![corr_types.len()], corr_types.to_vec()).unwrap(),
            )),
        ),
        RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
        RecordField::new(
            "NUM_CORR",
            Value::Scalar(ScalarValue::Int32(corr_types.len() as i32)),
        ),
    ]);
    table.add_row(row).unwrap();
}

fn add_data_description_row(ms: &mut MeasurementSet) {
    add_data_description_mapping_row(ms, 0, 0);
}

fn add_data_description_mapping_row(ms: &mut MeasurementSet, polarization_id: i32, spw_id: i32) {
    let table = ms
        .subtable_mut(crate::SubtableId::DataDescription)
        .expect("DATA_DESCRIPTION table");
    let row = RecordValue::new(vec![
        RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
        RecordField::new(
            "POLARIZATION_ID",
            Value::Scalar(ScalarValue::Int32(polarization_id)),
        ),
        RecordField::new(
            "SPECTRAL_WINDOW_ID",
            Value::Scalar(ScalarValue::Int32(spw_id)),
        ),
    ]);
    table.add_row(row).unwrap();
}

fn add_state_row(ms: &mut MeasurementSet, obs_mode: &str) {
    let table = ms
        .subtable_mut(crate::SubtableId::State)
        .expect("STATE table");
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("CAL", Value::Scalar(ScalarValue::Float64(0.0))),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("LOAD", Value::Scalar(ScalarValue::Float64(0.0))),
            RecordField::new(
                "OBS_MODE",
                Value::Scalar(ScalarValue::String(obs_mode.to_string())),
            ),
            RecordField::new("REF", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("SIG", Value::Scalar(ScalarValue::Bool(true))),
            RecordField::new("SUB_SCAN", Value::Scalar(ScalarValue::Int32(0))),
        ]))
        .unwrap();
}

fn add_antenna_rows(ms: &mut MeasurementSet) {
    let mut antenna = ms.antenna_mut().expect("ANTENNA accessor");
    antenna
        .add_antenna(
            "VLA01",
            "N01",
            "GROUND-BASED",
            "ALT-AZ",
            [0.0, 10.0, 20.0],
            [0.0, 0.0, 0.0],
            25.0,
        )
        .unwrap();
    antenna
        .add_antenna(
            "VLA02",
            "N02",
            "GROUND-BASED",
            "ALT-AZ",
            [1.0, 11.0, 21.0],
            [0.0, 0.0, 0.0],
            25.0,
        )
        .unwrap();
}

fn add_main_row(ms: &mut MeasurementSet, time: f64, antenna2: i32, scan_number: i32) {
    add_main_row_with_selection(ms, time, antenna2, scan_number, 0, -1, [0.0, 0.0, 0.0]);
}

fn add_main_row_with_selection(
    ms: &mut MeasurementSet,
    time: f64,
    antenna2: i32,
    scan_number: i32,
    data_desc_id: i32,
    state_id: i32,
    uvw: [f64; 3],
) {
    let schema = ms.main_table().schema().unwrap().clone();
    let fields = schema
        .columns()
        .iter()
        .map(|column| match column.name() {
            "ANTENNA1" => RecordField::new("ANTENNA1", Value::Scalar(ScalarValue::Int32(0))),
            "ANTENNA2" => RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(antenna2))),
            "ARRAY_ID" => RecordField::new("ARRAY_ID", Value::Scalar(ScalarValue::Int32(0))),
            "DATA_DESC_ID" => RecordField::new(
                "DATA_DESC_ID",
                Value::Scalar(ScalarValue::Int32(data_desc_id)),
            ),
            "FIELD_ID" => RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(0))),
            "INTERVAL" => RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(15.0))),
            "EXPOSURE" => RecordField::new("EXPOSURE", Value::Scalar(ScalarValue::Float64(15.0))),
            "OBSERVATION_ID" => {
                RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(0)))
            }
            "SCAN_NUMBER" => RecordField::new(
                "SCAN_NUMBER",
                Value::Scalar(ScalarValue::Int32(scan_number)),
            ),
            "STATE_ID" => RecordField::new("STATE_ID", Value::Scalar(ScalarValue::Int32(state_id))),
            "TIME" => RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(time))),
            "TIME_CENTROID" => {
                RecordField::new("TIME_CENTROID", Value::Scalar(ScalarValue::Float64(time)))
            }
            "UVW" => RecordField::new(
                "UVW",
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![3], uvw.to_vec()).unwrap(),
                )),
            ),
            name => RecordField::new(name, default_value(name)),
        })
        .collect();
    ms.main_table_mut()
        .add_row(RecordValue::new(fields))
        .unwrap();
}

fn set_main_row_flag_matrix(ms: &mut MeasurementSet, row: usize, flags: ArrayD<bool>) {
    ms.main_table_mut()
        .cell_accessor_mut(row, "FLAG")
        .unwrap()
        .set(Value::Array(ArrayValue::Bool(flags)))
        .unwrap();
}
