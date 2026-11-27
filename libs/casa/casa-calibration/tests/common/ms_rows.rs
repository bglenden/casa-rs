// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn add_apply_fixture_row(
    ms: &mut MeasurementSet,
    data_desc_id: i32,
    field_id: i32,
    scan_number: i32,
    time: f64,
) {
    let schema = ms.main_table().schema().expect("main schema").clone();
    let has_weight_spectrum = schema.contains_column("WEIGHT_SPECTRUM");
    let row = RecordValue::new(
        schema
            .columns()
            .iter()
            .map(|column| match column.name() {
                "ANTENNA1" => RecordField::new("ANTENNA1", Value::Scalar(ScalarValue::Int32(0))),
                "ANTENNA2" => RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(1))),
                "ARRAY_ID" => RecordField::new("ARRAY_ID", Value::Scalar(ScalarValue::Int32(0))),
                "DATA_DESC_ID" => RecordField::new(
                    "DATA_DESC_ID",
                    Value::Scalar(ScalarValue::Int32(data_desc_id)),
                ),
                "EXPOSURE" => {
                    RecordField::new("EXPOSURE", Value::Scalar(ScalarValue::Float64(10.0)))
                }
                "FIELD_ID" => {
                    RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(field_id)))
                }
                "FLAG" => RecordField::new(
                    "FLAG",
                    Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![false; 4]).unwrap(),
                    )),
                ),
                "FLAG_ROW" => RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                "INTERVAL" => {
                    RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(10.0)))
                }
                "OBSERVATION_ID" => {
                    RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(0)))
                }
                "SCAN_NUMBER" => RecordField::new(
                    "SCAN_NUMBER",
                    Value::Scalar(ScalarValue::Int32(scan_number)),
                ),
                "SIGMA" => RecordField::new(
                    "SIGMA",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(vec![2], vec![1.0, 1.0]).unwrap(),
                    )),
                ),
                "TIME" => RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(time))),
                "TIME_CENTROID" => {
                    RecordField::new("TIME_CENTROID", Value::Scalar(ScalarValue::Float64(time)))
                }
                "UVW" => RecordField::new(
                    "UVW",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![3], vec![0.0, 1.0, 2.0]).unwrap(),
                    )),
                ),
                "WEIGHT" => RecordField::new(
                    "WEIGHT",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(
                            vec![2],
                            if has_weight_spectrum {
                                vec![7.0, 11.0]
                            } else {
                                vec![1.0, 1.0]
                            },
                        )
                        .unwrap(),
                    )),
                ),
                "WEIGHT_SPECTRUM" => RecordField::new(
                    "WEIGHT_SPECTRUM",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![1.0, 2.0, 1.0, 2.0])
                            .unwrap(),
                    )),
                ),
                "DATA" | "CORRECTED_DATA" => RecordField::new(
                    column.name(),
                    Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(
                            IxDyn(&[2, 2]).f(),
                            vec![
                                Complex32::new(1.0, 0.0),
                                Complex32::new(0.0, 1.0),
                                Complex32::new(2.0, 0.0),
                                Complex32::new(0.0, 2.0),
                            ],
                        )
                        .unwrap(),
                    )),
                ),
                name => {
                    RecordField::new(name, default_value_for_column_name(name, schema.columns()))
                }
            })
            .collect(),
    );
    ms.main_table_mut().add_row(row).expect("add MAIN row");
}

pub(super) fn add_gain_solve_row(
    ms: &mut MeasurementSet,
    antenna1: i32,
    antenna2: i32,
    time: f64,
    kind: SyntheticGainFixtureKind,
) {
    let gains = gains_for_fixture_kind(kind);
    add_custom_gain_solve_row(ms, antenna1, antenna2, 0, 1, time, &gains);
}

pub(super) fn gains_for_fixture_kind(kind: SyntheticGainFixtureKind) -> [[Complex32; 2]; 3] {
    match kind {
        SyntheticGainFixtureKind::G => [
            [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
            [
                Complex32::new(0.9553365, 0.29552022),
                Complex32::new(0.921061, -0.38941833),
            ],
            [
                Complex32::new(0.9800666, -0.19866933),
                Complex32::new(0.87758255, 0.47942555),
            ],
        ],
        SyntheticGainFixtureKind::T => [
            [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
            [
                Complex32::new(0.9553365, 0.29552022),
                Complex32::new(0.9553365, 0.29552022),
            ],
            [
                Complex32::new(0.9800666, -0.19866933),
                Complex32::new(0.9800666, -0.19866933),
            ],
        ],
        SyntheticGainFixtureKind::GAmplitudePhase => [
            [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
            [
                Complex32::new(1.2419374, 0.38417628),
                Complex32::new(0.6907958, -0.29206374),
            ],
            [
                Complex32::new(0.83305657, -0.16873595),
                Complex32::new(1.3163738, 0.7191383),
            ],
        ],
        SyntheticGainFixtureKind::TAmplitudePhase => [
            [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
            [
                Complex32::new(1.2419374, 0.38417628),
                Complex32::new(1.2419374, 0.38417628),
            ],
            [
                Complex32::new(0.83305657, -0.16873595),
                Complex32::new(0.83305657, -0.16873595),
            ],
        ],
    }
}

pub(super) fn add_custom_gain_solve_row(
    ms: &mut MeasurementSet,
    antenna1: i32,
    antenna2: i32,
    field_id: i32,
    scan_number: i32,
    time: f64,
    gains: &[[Complex32; 2]; 3],
) {
    add_custom_gain_solve_row_with_model(
        ms,
        antenna1,
        antenna2,
        field_id,
        scan_number,
        time,
        gains,
        [Complex32::new(1.0, 0.0); 4],
    );
}

#[allow(clippy::too_many_arguments)]
pub(super) fn add_custom_gain_solve_row_with_model(
    ms: &mut MeasurementSet,
    antenna1: i32,
    antenna2: i32,
    field_id: i32,
    scan_number: i32,
    time: f64,
    gains: &[[Complex32; 2]; 3],
    model_values: [Complex32; 4],
) {
    let g1 = gains[usize::try_from(antenna1).expect("antenna1 index")];
    let g2 = gains[usize::try_from(antenna2).expect("antenna2 index")];
    let rr = g1[0] * g2[0].conj();
    let ll = g1[1] * g2[1].conj();
    let data_values = [
        rr * model_values[0],
        ll * model_values[1],
        rr * model_values[2],
        ll * model_values[3],
    ];

    let row = RecordValue::new(
        ms.main_table()
            .schema()
            .expect("main schema")
            .columns()
            .iter()
            .map(|column| match column.name() {
                "ANTENNA1" => {
                    RecordField::new("ANTENNA1", Value::Scalar(ScalarValue::Int32(antenna1)))
                }
                "ANTENNA2" => {
                    RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(antenna2)))
                }
                "ARRAY_ID" => RecordField::new("ARRAY_ID", Value::Scalar(ScalarValue::Int32(0))),
                "DATA_DESC_ID" => {
                    RecordField::new("DATA_DESC_ID", Value::Scalar(ScalarValue::Int32(0)))
                }
                "EXPOSURE" => {
                    RecordField::new("EXPOSURE", Value::Scalar(ScalarValue::Float64(10.0)))
                }
                "FIELD_ID" => {
                    RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(field_id)))
                }
                "FLAG" => RecordField::new(
                    "FLAG",
                    Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![false; 4]).unwrap(),
                    )),
                ),
                "FLAG_ROW" => RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                "INTERVAL" => {
                    RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(10.0)))
                }
                "OBSERVATION_ID" => {
                    RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(0)))
                }
                "SCAN_NUMBER" => RecordField::new(
                    "SCAN_NUMBER",
                    Value::Scalar(ScalarValue::Int32(scan_number)),
                ),
                "SIGMA" => RecordField::new(
                    "SIGMA",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(vec![2], vec![1.0, 1.0]).unwrap(),
                    )),
                ),
                "TIME" => RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(time))),
                "TIME_CENTROID" => {
                    RecordField::new("TIME_CENTROID", Value::Scalar(ScalarValue::Float64(time)))
                }
                "UVW" => RecordField::new(
                    "UVW",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![3], vec![0.0, 1.0, 2.0]).unwrap(),
                    )),
                ),
                "WEIGHT" => RecordField::new(
                    "WEIGHT",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(vec![2], vec![1.0, 1.0]).unwrap(),
                    )),
                ),
                "DATA" => RecordField::new(
                    "DATA",
                    Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), data_values.to_vec()).unwrap(),
                    )),
                ),
                "MODEL_DATA" => RecordField::new(
                    "MODEL_DATA",
                    Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), model_values.to_vec()).unwrap(),
                    )),
                ),
                name => RecordField::new(
                    name,
                    default_value_for_column_name(
                        name,
                        ms.main_table().schema().expect("main schema").columns(),
                    ),
                ),
            })
            .collect(),
    );
    ms.main_table_mut()
        .add_row(row)
        .expect("add gain solve row");
}

#[allow(clippy::too_many_arguments)]
pub(super) fn add_custom_bandpass_solve_row(
    ms: &mut MeasurementSet,
    field_id: i32,
    antenna1: i32,
    antenna2: i32,
    scan_number: i32,
    time: f64,
    prior_gains: &[[Complex32; 2]; 3],
    bandpass_gains: &[[[Complex32; 2]; 2]; 3],
) {
    let g1 = prior_gains[usize::try_from(antenna1).expect("antenna1 index")];
    let g2 = prior_gains[usize::try_from(antenna2).expect("antenna2 index")];
    let b1 = bandpass_gains[usize::try_from(antenna1).expect("antenna1 index")];
    let b2 = bandpass_gains[usize::try_from(antenna2).expect("antenna2 index")];

    let rr0 = (g1[0] * b1[0][0]) * (g2[0] * b2[0][0]).conj();
    let ll0 = (g1[1] * b1[1][0]) * (g2[1] * b2[1][0]).conj();
    let rr1 = (g1[0] * b1[0][1]) * (g2[0] * b2[0][1]).conj();
    let ll1 = (g1[1] * b1[1][1]) * (g2[1] * b2[1][1]).conj();

    let row = RecordValue::new(
        ms.main_table()
            .schema()
            .expect("main schema")
            .columns()
            .iter()
            .map(|column| match column.name() {
                "ANTENNA1" => {
                    RecordField::new("ANTENNA1", Value::Scalar(ScalarValue::Int32(antenna1)))
                }
                "ANTENNA2" => {
                    RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(antenna2)))
                }
                "ARRAY_ID" => RecordField::new("ARRAY_ID", Value::Scalar(ScalarValue::Int32(0))),
                "DATA_DESC_ID" => {
                    RecordField::new("DATA_DESC_ID", Value::Scalar(ScalarValue::Int32(0)))
                }
                "EXPOSURE" => {
                    RecordField::new("EXPOSURE", Value::Scalar(ScalarValue::Float64(10.0)))
                }
                "FIELD_ID" => {
                    RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(field_id)))
                }
                "FLAG" => RecordField::new(
                    "FLAG",
                    Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![false; 4]).unwrap(),
                    )),
                ),
                "FLAG_ROW" => RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                "INTERVAL" => {
                    RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(10.0)))
                }
                "OBSERVATION_ID" => {
                    RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(0)))
                }
                "SCAN_NUMBER" => RecordField::new(
                    "SCAN_NUMBER",
                    Value::Scalar(ScalarValue::Int32(scan_number)),
                ),
                "SIGMA" => RecordField::new(
                    "SIGMA",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(vec![2], vec![1.0, 1.0]).unwrap(),
                    )),
                ),
                "TIME" => RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(time))),
                "TIME_CENTROID" => {
                    RecordField::new("TIME_CENTROID", Value::Scalar(ScalarValue::Float64(time)))
                }
                "UVW" => RecordField::new(
                    "UVW",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![3], vec![0.0, 1.0, 2.0]).unwrap(),
                    )),
                ),
                "WEIGHT" => RecordField::new(
                    "WEIGHT",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(vec![2], vec![1.0, 1.0]).unwrap(),
                    )),
                ),
                "DATA" => RecordField::new(
                    "DATA",
                    Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![rr0, ll0, rr1, ll1])
                            .unwrap(),
                    )),
                ),
                name => RecordField::new(
                    name,
                    default_value_for_column_name(
                        name,
                        ms.main_table().schema().expect("main schema").columns(),
                    ),
                ),
            })
            .collect(),
    );
    ms.main_table_mut()
        .add_row(row)
        .expect("add bandpass solve row");
}

pub(super) fn set_field_directions_in_table(path: &Path, directions: &[(usize, f64, f64)]) {
    let mut table = Table::open(TableOptions::new(path)).expect("open FIELD table");
    for &(row, ra, dec) in directions {
        let direction =
            ArrayValue::Float64(ArrayD::from_shape_vec(vec![2, 1], vec![ra, dec]).unwrap());
        for column in ["DELAY_DIR", "PHASE_DIR", "REFERENCE_DIR"] {
            table
                .cell_accessor_mut(row, column)
                .and_then(|mut cell| cell.set(Value::Array(direction.clone())))
                .expect("set FIELD direction");
        }
    }
    table
        .save(TableOptions::new(path))
        .expect("save FIELD table");
}
