// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub struct SyntheticGainSolutionRow {
    pub time_seconds: f64,
    pub field_id: i32,
    pub spectral_window_id: i32,
    pub antenna_id: i32,
    pub gains: Vec<Complex32>,
    pub flags: Vec<bool>,
}

pub struct SyntheticDelaySolutionRow {
    pub time_seconds: f64,
    pub field_id: i32,
    pub spectral_window_id: i32,
    pub antenna_id: i32,
    pub delays_ns: Vec<f32>,
    pub flags: Vec<bool>,
}

pub struct SyntheticBPolySolutionRow {
    pub time_seconds: f64,
    pub field_id: i32,
    pub spectral_window_id: i32,
    pub antenna_id: i32,
    pub scale_factor: Complex32,
    pub valid_domain_hz: [f64; 2],
    pub amp_coefficients: Vec<Vec<f64>>,
    pub phase_coefficients: Vec<Vec<f64>>,
    pub phase_units: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub enum SyntheticGainFixtureKind {
    G,
    T,
    GAmplitudePhase,
    TAmplitudePhase,
}

pub struct SyntheticGainTimeCluster {
    pub time_seconds: f64,
    pub scan_number: i32,
    pub gains: [[Complex32; 2]; 3],
}

pub struct SyntheticBandpassTimeCluster {
    pub field_id: i32,
    pub time_seconds: f64,
    pub scan_number: i32,
    pub prior_gains: [[Complex32; 2]; 3],
    pub bandpass_gains: [[[Complex32; 2]; 2]; 3],
}

pub fn create_apply_gain_caltable(
    root: &Path,
    field_names: &[&str],
    rows: &[SyntheticGainSolutionRow],
) -> PathBuf {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("TIME", casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar("FIELD_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("SPECTRAL_WINDOW_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA1", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA2", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("INTERVAL", casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar("SCAN_NUMBER", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("OBSERVATION_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("ARRAY_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("TIME_EXTRA_PREC", casa_core::PrimitiveType::Float64),
        ColumnSchema::array_variable("CPARAM", casa_core::PrimitiveType::Complex32, Some(1)),
        ColumnSchema::array_variable("PARAMERR", casa_core::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable("FLAG", casa_core::PrimitiveType::Bool, Some(1)),
        ColumnSchema::array_variable("SNR", casa_core::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable("WEIGHT", casa_core::PrimitiveType::Float32, Some(1)),
    ])
    .expect("valid gain caltable schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "Calibration".to_string(),
        sub_type: "G Jones".to_string(),
        readme: Vec::new(),
    });
    table.keywords_mut().upsert(
        "ParType",
        Value::Scalar(ScalarValue::String("Complex".to_string())),
    );
    table.keywords_mut().upsert(
        "VisCal",
        Value::Scalar(ScalarValue::String("G Jones".to_string())),
    );
    table.keywords_mut().upsert(
        "MSName",
        Value::Scalar(ScalarValue::String("apply_fixture.ms".to_string())),
    );
    table.keywords_mut().upsert(
        "PolBasis",
        Value::Scalar(ScalarValue::String("unknown".to_string())),
    );
    table.keywords_mut().upsert(
        "CASA_Version",
        Value::Scalar(ScalarValue::String("test".to_string())),
    );
    set_fixed_unit_keyword(&mut table, "TIME", &["s"]);
    set_measinfo_keyword(&mut table, "TIME", "epoch", Some("UTC"));
    set_fixed_unit_keyword(&mut table, "INTERVAL", &["s"]);
    set_fixed_unit_keyword(&mut table, "TIME_EXTRA_PREC", &["s"]);
    for name in [
        "OBSERVATION",
        "ANTENNA",
        "FIELD",
        "SPECTRAL_WINDOW",
        "HISTORY",
    ] {
        table
            .keywords_mut()
            .upsert(name, Value::table_ref(format!("././{name}")));
    }

    for row in rows {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "TIME",
                    Value::Scalar(ScalarValue::Float64(row.time_seconds)),
                ),
                RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(row.field_id))),
                RecordField::new(
                    "SPECTRAL_WINDOW_ID",
                    Value::Scalar(ScalarValue::Int32(row.spectral_window_id)),
                ),
                RecordField::new(
                    "ANTENNA1",
                    Value::Scalar(ScalarValue::Int32(row.antenna_id)),
                ),
                RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(-1))),
                RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(30.0))),
                RecordField::new("SCAN_NUMBER", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("ARRAY_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("TIME_EXTRA_PREC", Value::Scalar(ScalarValue::Float64(0.0))),
                RecordField::new(
                    "CPARAM",
                    Value::Array(ArrayValue::from_complex32_vec(row.gains.clone())),
                ),
                RecordField::new(
                    "PARAMERR",
                    Value::Array(ArrayValue::from_f32_vec(vec![0.1; row.gains.len()])),
                ),
                RecordField::new(
                    "FLAG",
                    Value::Array(ArrayValue::from_bool_vec(row.flags.clone())),
                ),
                RecordField::new(
                    "SNR",
                    Value::Array(ArrayValue::from_f32_vec(vec![10.0; row.gains.len()])),
                ),
                RecordField::new(
                    "WEIGHT",
                    Value::Array(ArrayValue::from_f32_vec(vec![1.0; row.gains.len()])),
                ),
            ]))
            .expect("insert gain solution row");
    }

    table
        .save(TableOptions::new(root).with_data_manager(DataManagerKind::StandardStMan))
        .expect("save gain caltable");

    let spectral_window_ids = rows
        .iter()
        .map(|row| row.spectral_window_id)
        .collect::<Vec<_>>();
    write_caltable_field_subtable(root.join("FIELD"), field_names);
    write_caltable_spectral_window_subtable(root.join("SPECTRAL_WINDOW"), &spectral_window_ids);
    write_minimal_observation_table(root.join("OBSERVATION"));
    let antenna_count = rows
        .iter()
        .map(|row| row.antenna_id)
        .max()
        .map_or(0usize, |max| usize::try_from(max + 1).unwrap_or(0));
    write_minimal_antenna_table(root.join("ANTENNA"), antenna_count.max(1));
    write_empty_table(root.join("HISTORY"));

    root.to_path_buf()
}

pub fn create_apply_delay_caltable(
    root: &Path,
    field_names: &[&str],
    rows: &[SyntheticDelaySolutionRow],
) -> PathBuf {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("TIME", casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar("FIELD_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("SPECTRAL_WINDOW_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA1", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA2", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("INTERVAL", casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar("SCAN_NUMBER", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("OBSERVATION_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("ARRAY_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("TIME_EXTRA_PREC", casa_core::PrimitiveType::Float64),
        ColumnSchema::array_variable("FPARAM", casa_core::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable("FLAG", casa_core::PrimitiveType::Bool, Some(1)),
    ])
    .expect("valid delay caltable schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "Calibration".to_string(),
        sub_type: "K Jones".to_string(),
        readme: Vec::new(),
    });
    table.keywords_mut().upsert(
        "ParType",
        Value::Scalar(ScalarValue::String("Float".to_string())),
    );
    table.keywords_mut().upsert(
        "VisCal",
        Value::Scalar(ScalarValue::String("K Jones".to_string())),
    );
    table.keywords_mut().upsert(
        "MSName",
        Value::Scalar(ScalarValue::String("apply_fixture.ms".to_string())),
    );
    table.keywords_mut().upsert(
        "PolBasis",
        Value::Scalar(ScalarValue::String("unknown".to_string())),
    );
    table.keywords_mut().upsert(
        "CASA_Version",
        Value::Scalar(ScalarValue::String("test".to_string())),
    );
    set_fixed_unit_keyword(&mut table, "TIME", &["s"]);
    set_measinfo_keyword(&mut table, "TIME", "epoch", Some("UTC"));
    set_fixed_unit_keyword(&mut table, "INTERVAL", &["s"]);
    set_fixed_unit_keyword(&mut table, "TIME_EXTRA_PREC", &["s"]);
    set_fixed_unit_keyword(&mut table, "FPARAM", &["ns"]);
    for name in [
        "OBSERVATION",
        "ANTENNA",
        "FIELD",
        "SPECTRAL_WINDOW",
        "HISTORY",
    ] {
        table
            .keywords_mut()
            .upsert(name, Value::table_ref(format!("././{name}")));
    }

    for row in rows {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "TIME",
                    Value::Scalar(ScalarValue::Float64(row.time_seconds)),
                ),
                RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(row.field_id))),
                RecordField::new(
                    "SPECTRAL_WINDOW_ID",
                    Value::Scalar(ScalarValue::Int32(row.spectral_window_id)),
                ),
                RecordField::new(
                    "ANTENNA1",
                    Value::Scalar(ScalarValue::Int32(row.antenna_id)),
                ),
                RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(-1))),
                RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(30.0))),
                RecordField::new("SCAN_NUMBER", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("ARRAY_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("TIME_EXTRA_PREC", Value::Scalar(ScalarValue::Float64(0.0))),
                RecordField::new(
                    "FPARAM",
                    Value::Array(ArrayValue::from_f32_vec(row.delays_ns.clone())),
                ),
                RecordField::new(
                    "FLAG",
                    Value::Array(ArrayValue::from_bool_vec(row.flags.clone())),
                ),
            ]))
            .expect("insert delay solution row");
    }

    table
        .save(TableOptions::new(root).with_data_manager(DataManagerKind::StandardStMan))
        .expect("save delay caltable");

    let spectral_window_ids = rows
        .iter()
        .map(|row| row.spectral_window_id)
        .collect::<Vec<_>>();
    write_caltable_field_subtable(root.join("FIELD"), field_names);
    write_delay_caltable_spectral_window_subtable(
        root.join("SPECTRAL_WINDOW"),
        &spectral_window_ids,
    );
    write_minimal_observation_table(root.join("OBSERVATION"));
    let antenna_count = rows
        .iter()
        .map(|row| row.antenna_id)
        .max()
        .map_or(0usize, |max| usize::try_from(max + 1).unwrap_or(0));
    write_minimal_antenna_table(root.join("ANTENNA"), antenna_count.max(1));
    write_empty_table(root.join("HISTORY"));

    root.to_path_buf()
}

pub fn create_apply_bpoly_caltable(root: &Path, rows: &[SyntheticBPolySolutionRow]) -> PathBuf {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("TIME", casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar("FIELD_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA1", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("INTERVAL", casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar("CAL_DESC_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("SCALE_FACTOR", casa_core::PrimitiveType::Complex32),
        ColumnSchema::scalar("N_POLY_AMP", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("N_POLY_PHASE", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("PHASE_UNITS", casa_core::PrimitiveType::String),
        ColumnSchema::array_variable("VALID_DOMAIN", casa_core::PrimitiveType::Float64, Some(1)),
        ColumnSchema::array_variable("POLY_COEFF_AMP", casa_core::PrimitiveType::Float64, Some(4)),
        ColumnSchema::array_variable(
            "POLY_COEFF_PHASE",
            casa_core::PrimitiveType::Float64,
            Some(4),
        ),
    ])
    .expect("valid BPOLY schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "Calibration".to_string(),
        sub_type: "BPOLY".to_string(),
        readme: Vec::new(),
    });
    table
        .keywords_mut()
        .upsert("CAL_DESC", Value::table_ref("././CAL_DESC"));
    table
        .keywords_mut()
        .upsert("CAL_HISTORY", Value::table_ref("././CAL_HISTORY"));
    set_fixed_unit_keyword(&mut table, "TIME", &["s"]);
    set_measinfo_keyword(&mut table, "TIME", "epoch", Some("UTC"));
    set_fixed_unit_keyword(&mut table, "INTERVAL", &["s"]);
    set_fixed_unit_keyword(&mut table, "VALID_DOMAIN", &["Hz"]);

    let mut cal_desc_ids = std::collections::BTreeMap::new();
    for row in rows {
        let next_id = i32::try_from(cal_desc_ids.len()).expect("small synthetic CAL_DESC table");
        cal_desc_ids
            .entry(row.spectral_window_id)
            .or_insert(next_id);
    }

    for row in rows {
        assert_eq!(
            row.amp_coefficients.len(),
            row.phase_coefficients.len(),
            "BPOLY amp/phase receptor counts must match"
        );
        let receptor_count = row.amp_coefficients.len().max(1);
        let amp_degree = row
            .amp_coefficients
            .first()
            .map_or(0_i32, |coefficients| coefficients.len() as i32);
        let phase_degree = row
            .phase_coefficients
            .first()
            .map_or(0_i32, |coefficients| coefficients.len() as i32);
        assert!(
            row.amp_coefficients
                .iter()
                .all(|coefficients| coefficients.len() as i32 == amp_degree)
        );
        assert!(
            row.phase_coefficients
                .iter()
                .all(|coefficients| coefficients.len() as i32 == phase_degree)
        );
        let flat_amp = row
            .amp_coefficients
            .iter()
            .flat_map(|coefficients| coefficients.iter().copied())
            .collect::<Vec<_>>();
        let flat_phase = row
            .phase_coefficients
            .iter()
            .flat_map(|coefficients| coefficients.iter().copied())
            .collect::<Vec<_>>();
        let amp_array = ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[1, 1, 1, flat_amp.len()]).f(), flat_amp).unwrap(),
        );
        let phase_array = ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[1, 1, 1, flat_phase.len()]).f(), flat_phase).unwrap(),
        );
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "TIME",
                    Value::Scalar(ScalarValue::Float64(row.time_seconds)),
                ),
                RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(row.field_id))),
                RecordField::new(
                    "ANTENNA1",
                    Value::Scalar(ScalarValue::Int32(row.antenna_id)),
                ),
                RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(30.0))),
                RecordField::new(
                    "CAL_DESC_ID",
                    Value::Scalar(ScalarValue::Int32(
                        *cal_desc_ids
                            .get(&row.spectral_window_id)
                            .expect("synthetic CAL_DESC entry"),
                    )),
                ),
                RecordField::new(
                    "SCALE_FACTOR",
                    Value::Scalar(ScalarValue::Complex32(row.scale_factor)),
                ),
                RecordField::new("N_POLY_AMP", Value::Scalar(ScalarValue::Int32(amp_degree))),
                RecordField::new(
                    "N_POLY_PHASE",
                    Value::Scalar(ScalarValue::Int32(phase_degree)),
                ),
                RecordField::new(
                    "PHASE_UNITS",
                    Value::Scalar(ScalarValue::String(row.phase_units.to_string())),
                ),
                RecordField::new(
                    "VALID_DOMAIN",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(
                            IxDyn(&[2]).f(),
                            vec![row.valid_domain_hz[0], row.valid_domain_hz[1]],
                        )
                        .unwrap(),
                    )),
                ),
                RecordField::new("POLY_COEFF_AMP", Value::Array(amp_array)),
                RecordField::new("POLY_COEFF_PHASE", Value::Array(phase_array)),
            ]))
            .expect("insert BPOLY solution row");
        assert!(receptor_count > 0);
    }

    table
        .save(TableOptions::new(root).with_data_manager(DataManagerKind::StandardStMan))
        .expect("save BPOLY caltable");

    write_bpoly_cal_desc_subtable(root.join("CAL_DESC"), &cal_desc_ids, rows);
    write_empty_table(root.join("CAL_HISTORY"));

    root.to_path_buf()
}

pub(super) fn write_caltable_field_subtable(path: PathBuf, field_names: &[&str]) {
    let schema = build_table_schema(ms_schema::field::REQUIRED_COLUMNS).expect("FIELD schema");
    let mut table = Table::with_schema(schema);
    for (field_id, name) in field_names.iter().enumerate() {
        let direction = ArrayValue::Float64(
            ArrayD::from_shape_vec(vec![2, 1], vec![1.0 + field_id as f64, 0.5]).unwrap(),
        );
        let row = make_subtable_row(
            ms_schema::field::REQUIRED_COLUMNS,
            &[
                (
                    "NAME",
                    Value::Scalar(ScalarValue::String((*name).to_string())),
                ),
                ("CODE", Value::Scalar(ScalarValue::String("T".to_string()))),
                ("NUM_POLY", Value::Scalar(ScalarValue::Int32(0))),
                ("DELAY_DIR", Value::Array(direction.clone())),
                ("PHASE_DIR", Value::Array(direction.clone())),
                ("REFERENCE_DIR", Value::Array(direction)),
                (
                    "SOURCE_ID",
                    Value::Scalar(ScalarValue::Int32(field_id as i32)),
                ),
                ("TIME", Value::Scalar(ScalarValue::Float64(100.0))),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
        table.add_row(row).expect("add caltable FIELD row");
    }
    table
        .save(TableOptions::new(path))
        .expect("save FIELD subtable");
}

pub(super) fn write_caltable_spectral_window_subtable(path: PathBuf, spectral_window_ids: &[i32]) {
    let schema =
        build_table_schema(ms_schema::spectral_window::REQUIRED_COLUMNS).expect("SPW schema");
    let mut table = Table::with_schema(schema);
    let max_spw = spectral_window_ids.iter().copied().max().unwrap_or(-1);
    for spw_id in 0..=max_spw {
        let base_frequency_hz = 1.0e9_f64 + spw_id as f64 * 1.0e8;
        let row = make_subtable_row(
            ms_schema::spectral_window::REQUIRED_COLUMNS,
            &[
                ("NUM_CHAN", Value::Scalar(ScalarValue::Int32(2))),
                (
                    "CHAN_FREQ",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(
                            vec![2],
                            vec![base_frequency_hz, base_frequency_hz + 1.0e6],
                        )
                        .unwrap(),
                    )),
                ),
                (
                    "CHAN_WIDTH",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![2], vec![1.0e6, 1.0e6]).unwrap(),
                    )),
                ),
                (
                    "EFFECTIVE_BW",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![2], vec![1.0e6, 1.0e6]).unwrap(),
                    )),
                ),
                (
                    "RESOLUTION",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![2], vec![1.0e6, 1.0e6]).unwrap(),
                    )),
                ),
                (
                    "REF_FREQUENCY",
                    Value::Scalar(ScalarValue::Float64(base_frequency_hz)),
                ),
                (
                    "TOTAL_BANDWIDTH",
                    Value::Scalar(ScalarValue::Float64(2.0e6)),
                ),
                ("MEAS_FREQ_REF", Value::Scalar(ScalarValue::Int32(5))),
                (
                    "NAME",
                    Value::Scalar(ScalarValue::String(format!("CALSPW{spw_id}"))),
                ),
                ("NET_SIDEBAND", Value::Scalar(ScalarValue::Int32(1))),
                ("FREQ_GROUP", Value::Scalar(ScalarValue::Int32(0))),
                (
                    "FREQ_GROUP_NAME",
                    Value::Scalar(ScalarValue::String("GROUP".to_string())),
                ),
                ("IF_CONV_CHAIN", Value::Scalar(ScalarValue::Int32(0))),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
        table.add_row(row).expect("add caltable SPW row");
    }
    table
        .save(TableOptions::new(path))
        .expect("save SPECTRAL_WINDOW subtable");
}

pub(super) fn write_delay_caltable_spectral_window_subtable(
    path: PathBuf,
    spectral_window_ids: &[i32],
) {
    let schema =
        build_table_schema(ms_schema::spectral_window::REQUIRED_COLUMNS).expect("SPW schema");
    let mut table = Table::with_schema(schema);
    let max_spw = spectral_window_ids.iter().copied().max().unwrap_or(-1);
    for spw_id in 0..=max_spw {
        let base_frequency_hz = 1.0e9_f64 + spw_id as f64 * 1.0e8;
        let row = make_subtable_row(
            ms_schema::spectral_window::REQUIRED_COLUMNS,
            &[
                ("NUM_CHAN", Value::Scalar(ScalarValue::Int32(1))),
                (
                    "CHAN_FREQ",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![1], vec![base_frequency_hz]).unwrap(),
                    )),
                ),
                (
                    "CHAN_WIDTH",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![1], vec![1.0e6]).unwrap(),
                    )),
                ),
                (
                    "EFFECTIVE_BW",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![1], vec![1.0e6]).unwrap(),
                    )),
                ),
                (
                    "RESOLUTION",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![1], vec![1.0e6]).unwrap(),
                    )),
                ),
                (
                    "REF_FREQUENCY",
                    Value::Scalar(ScalarValue::Float64(base_frequency_hz)),
                ),
                (
                    "TOTAL_BANDWIDTH",
                    Value::Scalar(ScalarValue::Float64(1.0e6)),
                ),
                ("MEAS_FREQ_REF", Value::Scalar(ScalarValue::Int32(5))),
                (
                    "NAME",
                    Value::Scalar(ScalarValue::String(format!("CALSPW{spw_id}"))),
                ),
                ("NET_SIDEBAND", Value::Scalar(ScalarValue::Int32(1))),
                ("FREQ_GROUP", Value::Scalar(ScalarValue::Int32(0))),
                (
                    "FREQ_GROUP_NAME",
                    Value::Scalar(ScalarValue::String("GROUP".to_string())),
                ),
                ("IF_CONV_CHAIN", Value::Scalar(ScalarValue::Int32(0))),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
        table.add_row(row).expect("add delay caltable SPW row");
    }
    table
        .save(TableOptions::new(path))
        .expect("save delay SPECTRAL_WINDOW subtable");
}

pub(super) fn write_bpoly_cal_desc_subtable(
    path: PathBuf,
    cal_desc_ids: &std::collections::BTreeMap<i32, i32>,
    rows: &[SyntheticBPolySolutionRow],
) {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("NUM_RECEPTORS", casa_core::PrimitiveType::Int32),
        ColumnSchema::array_variable(
            "SPECTRAL_WINDOW_ID",
            casa_core::PrimitiveType::Int32,
            Some(1),
        ),
    ])
    .expect("BPOLY CAL_DESC schema");
    let mut table = Table::with_schema(schema);
    for (spw_id, cal_desc_id) in cal_desc_ids {
        let receptor_count = rows
            .iter()
            .find(|row| row.spectral_window_id == *spw_id)
            .map(|row| row.amp_coefficients.len() as i32)
            .unwrap_or(1);
        while table.row_count() < usize::try_from(*cal_desc_id).expect("small CAL_DESC id") {
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("NUM_RECEPTORS", Value::Scalar(ScalarValue::Int32(1))),
                    RecordField::new(
                        "SPECTRAL_WINDOW_ID",
                        Value::Array(ArrayValue::from_i32_vec(vec![0])),
                    ),
                ]))
                .expect("pad CAL_DESC rows");
        }
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "NUM_RECEPTORS",
                    Value::Scalar(ScalarValue::Int32(receptor_count)),
                ),
                RecordField::new(
                    "SPECTRAL_WINDOW_ID",
                    Value::Array(ArrayValue::from_i32_vec(vec![*spw_id])),
                ),
            ]))
            .expect("add CAL_DESC row");
    }
    table
        .save(TableOptions::new(path))
        .expect("save BPOLY CAL_DESC subtable");
}
