// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn tiny_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "input.ms",
        MeasurementSetFixtureOptions::new(false, false, 1, 1, 2, 1, false),
    )
}

pub(super) fn multi_row_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "multi-row-input.ms",
        MeasurementSetFixtureOptions::new(false, false, 1, 1, 2, 8, false),
    )
}

pub(super) fn flagged_polarized_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "polarized-input.ms",
        MeasurementSetFixtureOptions::new(true, true, 2, 1, 2, 1, false),
    )
}

pub(super) fn vla_aw_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "vla-aw-input.ms",
        MeasurementSetFixtureOptions::new(true, false, 1, 1, 2, 1, false)
            .with_vla_observation_metadata(),
    )
}

pub(super) fn native_evla_measurement_set(root: &Path) -> PathBuf {
    let mut options = MeasurementSetFixtureOptions::new(true, false, 2, 2, 2, 8, false)
        .with_vla_observation_metadata();
    options.native_evla_cf = true;
    measurement_set_fixture(root, "native-evla-input.ms", options)
}

pub(super) fn two_pointing_vla_aw_measurement_set(root: &Path) -> PathBuf {
    let path = measurement_set_fixture(
        root,
        "two-pointing-vla-aw-input.ms",
        MeasurementSetFixtureOptions::new(true, false, 1, 1, 2, 2, false)
            .with_vla_observation_metadata()
            .with_two_fields(),
    );
    add_two_field_pointings(&path);
    path
}

pub(super) fn two_pointing_alma_spectral_measurement_set(root: &Path) -> PathBuf {
    let path = measurement_set_fixture(
        root,
        "two-pointing-alma-spectral-input.ms",
        MeasurementSetFixtureOptions::new(false, false, 32, 1, 2, 8, false)
            .with_alma_observation_metadata()
            .with_two_fields(),
    );
    add_two_field_pointings(&path);
    path
}

fn add_two_field_pointings(path: &Path) {
    let mut measurement_set = MeasurementSet::open(&path).expect("open two-pointing fixture");
    for row in 0..measurement_set.row_count() {
        let field = row % 2;
        let sign = if field == 0 { 1.0 } else { -1.0 };
        let field_direction = [1.0 + field as f64 * 1.0e-4, 0.5];
        let time = 59_000.0 * 86_400.0 + row as f64 * 10.0;
        for antenna in 0..2 {
            let antenna_delta = if antenna == 0 { -2.0e-6 } else { 2.0e-6 };
            let direction = Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(
                    vec![2, 1],
                    vec![
                        field_direction[0] + sign * 3.0e-5 + antenna_delta,
                        field_direction[1] + sign * 2.0e-5 + antenna_delta,
                    ],
                )
                .expect("POINTING direction shape"),
            ));
            measurement_set
                .subtable_mut(SubtableId::Pointing)
                .expect("POINTING")
                .add_row(required_row(
                    schema::pointing::REQUIRED_COLUMNS,
                    &[
                        ("ANTENNA_ID", int(antenna)),
                        ("DIRECTION", direction.clone()),
                        ("INTERVAL", float(10.0)),
                        ("NAME", string(&format!("FIELD_{field}_ANTENNA_{antenna}"))),
                        ("NUM_POLY", int(0)),
                        ("TARGET", direction),
                        ("TIME", float(time)),
                        ("TIME_ORIGIN", float(time)),
                        ("TRACKING", boolean(true)),
                    ],
                ))
                .expect("add POINTING row");
        }
    }
    measurement_set.save().expect("save two-pointing fixture");
}

pub(super) fn four_spw_vla_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "four-spw-vla-aw-input.ms",
        MeasurementSetFixtureOptions::new(true, false, 8, 4, 4, 24, false)
            .with_vla_observation_metadata(),
    )
}

pub(super) fn full_stokes_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "full-stokes-input.ms",
        MeasurementSetFixtureOptions::new(true, false, 2, 1, 27, 702, false),
    )
}

pub(super) fn unequal_linear_parallel_hand_measurement_set(
    root: &Path,
    name: &str,
    parallel_hand_weights: [f32; 2],
) -> PathBuf {
    measurement_set_fixture(
        root,
        name,
        MeasurementSetFixtureOptions::new(true, false, 1, 1, 2, 1, false)
            .with_linear_correlations()
            .with_parallel_hand_weights(parallel_hand_weights),
    )
}

pub(super) fn spectral_line_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "line-input.ms",
        MeasurementSetFixtureOptions::new(true, true, 4, 1, 2, 1, false),
    )
}

pub(super) fn thirty_two_channel_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "thirty-two-channel-input.ms",
        MeasurementSetFixtureOptions::new(false, false, 32, 1, 2, 1, false),
    )
}

pub(super) fn thirty_two_channel_multi_row_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "thirty-two-channel-multi-row-input.ms",
        MeasurementSetFixtureOptions::new(false, false, 32, 1, 2, 8, false).with_two_fields(),
    )
}

pub(super) fn joint_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "joint-input.ms",
        MeasurementSetFixtureOptions::new(false, false, 4, 1, 2, 1, false),
    )
}

pub(super) fn undefined_weight_spectrum_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "undefined-weight-spectrum.ms",
        MeasurementSetFixtureOptions::new(false, false, 1, 1, 2, 1, true),
    )
}

pub(super) fn four_spw_aca_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "four-spw-input.ms",
        MeasurementSetFixtureOptions::new(false, false, 8, 4, 4, 24, false)
            .with_aca_observation_metadata(),
    )
}

pub(super) fn alma_primary_beam_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "alma-primary-beam-input.ms",
        MeasurementSetFixtureOptions::new(false, false, 2, 2, 2, 8, false)
            .with_alma_observation_metadata(),
    )
}

#[derive(Clone, Copy)]
pub(super) struct MeasurementSetFixtureOptions {
    polarized: bool,
    flag_cross_hand: bool,
    channel_count: usize,
    spectral_window_count: usize,
    antenna_count: usize,
    main_row_count: usize,
    undefined_weight_spectrum: bool,
    linear_correlations: bool,
    parallel_hand_weights: Option<[f32; 2]>,
    telescope_name: Option<&'static str>,
    dish_diameter_m: f64,
    field_count: usize,
    native_evla_cf: bool,
}

impl MeasurementSetFixtureOptions {
    const fn new(
        polarized: bool,
        flag_cross_hand: bool,
        channel_count: usize,
        spectral_window_count: usize,
        antenna_count: usize,
        main_row_count: usize,
        undefined_weight_spectrum: bool,
    ) -> Self {
        Self {
            polarized,
            flag_cross_hand,
            channel_count,
            spectral_window_count,
            antenna_count,
            main_row_count,
            undefined_weight_spectrum,
            linear_correlations: false,
            parallel_hand_weights: None,
            telescope_name: None,
            dish_diameter_m: 25.0,
            field_count: 1,
            native_evla_cf: false,
        }
    }

    const fn with_linear_correlations(mut self) -> Self {
        self.linear_correlations = true;
        self
    }

    const fn with_parallel_hand_weights(mut self, weights: [f32; 2]) -> Self {
        self.parallel_hand_weights = Some(weights);
        self
    }

    const fn with_aca_observation_metadata(mut self) -> Self {
        self.telescope_name = Some("ALMA");
        self.dish_diameter_m = 7.0;
        self
    }

    const fn with_alma_observation_metadata(mut self) -> Self {
        self.telescope_name = Some("ALMA");
        self.dish_diameter_m = 12.0;
        self
    }

    const fn with_vla_observation_metadata(mut self) -> Self {
        self.telescope_name = Some("EVLA");
        self.dish_diameter_m = 25.0;
        self
    }

    const fn with_two_fields(mut self) -> Self {
        self.field_count = 2;
        self
    }
}

pub(super) fn measurement_set_fixture(
    root: &Path,
    name: &str,
    options: MeasurementSetFixtureOptions,
) -> PathBuf {
    let output = root.join(name);
    let mut builder = MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data);
    if options.undefined_weight_spectrum {
        builder = builder.with_main_column(OptionalMainColumn::WeightSpectrum);
    }
    if options.polarized {
        builder = builder.with_main_column(OptionalMainColumn::ModelData);
    }
    if options.channel_count == 4 {
        builder = builder.with_main_column(OptionalMainColumn::CorrectedData);
    }
    let mut measurement_set =
        MeasurementSet::create_memory(builder).expect("create in-memory application fixture");
    populate_fixture(&mut measurement_set, options);
    measurement_set
        .save_as(&output)
        .expect("persist fixture with production tiled bindings");
    initialize_measurement_set_owner_manifest(&output).expect("initialize MS owner manifest");
    MeasurementSet::open(&output)
        .expect("reopen owned fixture")
        .save()
        .expect("preserve owner manifest and production bindings");

    let persisted = MeasurementSet::open(&output).expect("inspect persisted fixture");
    for column in ["DATA", "FLAG", "WEIGHT"] {
        assert!(
            persisted
                .main_table()
                .data_manager_info()
                .iter()
                .any(|manager| manager.dm_type == "TiledShapeStMan"
                    && manager.columns.iter().any(|name| name == column)),
            "{column} must use TiledShapeStMan"
        );
    }
    output
}

pub(super) fn populate_fixture(
    measurement_set: &mut MeasurementSet,
    options: MeasurementSetFixtureOptions,
) {
    let MeasurementSetFixtureOptions {
        polarized,
        flag_cross_hand,
        channel_count,
        spectral_window_count,
        antenna_count,
        main_row_count,
        linear_correlations,
        parallel_hand_weights,
        telescope_name,
        dish_diameter_m,
        field_count,
        ..
    } = options;
    {
        let mut antennas = measurement_set.antenna_mut().expect("ANTENNA");
        for antenna in 0..antenna_count {
            let arm = (antenna % 3) as f64 * std::f64::consts::TAU / 3.0;
            let radius = 35.0 * (antenna / 3 + 1) as f64;
            antennas
                .add_antenna(
                    &format!("VLA{:02}", antenna + 1),
                    &format!("N{:02}", antenna + 1),
                    "GROUND-BASED",
                    "ALT-AZ",
                    [
                        -1_601_185.4 + radius * arm.cos(),
                        -5_041_977.5 + radius * arm.sin(),
                        3_554_875.9,
                    ],
                    [0.0; 3],
                    dish_diameter_m,
                )
                .expect("add fixture antenna");
        }
    }

    if let Some(telescope_name) = telescope_name {
        measurement_set
            .subtable_mut(SubtableId::Observation)
            .expect("OBSERVATION")
            .add_row(required_row(
                schema::observation::REQUIRED_COLUMNS,
                &[
                    ("TELESCOPE_NAME", string(telescope_name)),
                    (
                        "TIME_RANGE",
                        Value::Array(ArrayValue::Float64(
                            ArrayD::from_shape_vec(
                                vec![2],
                                vec![59_000.0 * 86_400.0, 59_000.0 * 86_400.0 + 10.0],
                            )
                            .expect("observation time-range shape"),
                        )),
                    ),
                    ("OBSERVER", string("casa-rs-test")),
                    ("PROJECT", string("synthetic-aca-mvc")),
                    ("RELEASE_DATE", float(59_000.0 * 86_400.0)),
                ],
            ))
            .expect("add OBSERVATION row");
    }

    let correlation_codes = if !polarized {
        vec![1]
    } else if linear_correlations {
        vec![9, 10, 11, 12]
    } else {
        vec![5, 6, 7, 8]
    };
    let correlation_count = correlation_codes.len();
    let correlation_products = if polarized {
        vec![0, 0, 0, 1, 1, 0, 1, 1]
    } else {
        vec![0, 0]
    };
    for field_id in 0..field_count {
        let direction = ArrayValue::Float64(
            ArrayD::from_shape_vec(vec![2, 1], vec![1.0 + field_id as f64 * 1.0e-4, 0.5])
                .expect("direction shape"),
        );
        measurement_set
            .subtable_mut(SubtableId::Field)
            .expect("FIELD")
            .add_row(required_row(
                schema::field::REQUIRED_COLUMNS,
                &[
                    ("NAME", string(&format!("APPLICATION_FIELD_{field_id}"))),
                    ("CODE", string("TARGET")),
                    ("NUM_POLY", int(0)),
                    ("DELAY_DIR", Value::Array(direction.clone())),
                    ("PHASE_DIR", Value::Array(direction.clone())),
                    ("REFERENCE_DIR", Value::Array(direction)),
                    ("SOURCE_ID", int(-1)),
                    ("TIME", float(59_000.0 * 86_400.0)),
                    ("FLAG_ROW", boolean(false)),
                ],
            ))
            .expect("add FIELD row");
    }

    measurement_set
        .subtable_mut(SubtableId::Polarization)
        .expect("POLARIZATION")
        .add_row(required_row(
            schema::polarization::REQUIRED_COLUMNS,
            &[
                ("NUM_CORR", int(correlation_count as i32)),
                (
                    "CORR_TYPE",
                    Value::Array(ArrayValue::Int32(
                        ArrayD::from_shape_vec(vec![correlation_count], correlation_codes)
                            .expect("correlation shape"),
                    )),
                ),
                (
                    "CORR_PRODUCT",
                    Value::Array(ArrayValue::Int32(
                        ArrayD::from_shape_vec(vec![2, correlation_count], correlation_products)
                            .expect("receptor-pair shape"),
                    )),
                ),
                ("FLAG_ROW", boolean(false)),
            ],
        ))
        .expect("add POLARIZATION row");

    for spw in 0..spectral_window_count {
        let first_frequency_hz = if options.native_evla_cf {
            3.0e9
        } else {
            44.0e9
        } + spw as f64 * 100.0e6;
        let frequency = Value::Array(ArrayValue::Float64(
            ArrayD::from_shape_vec(
                vec![channel_count],
                (0..channel_count)
                    .map(|channel| first_frequency_hz + channel as f64 * 1.0e6)
                    .collect(),
            )
            .expect("frequency shape"),
        ));
        let width = Value::Array(ArrayValue::Float64(
            ArrayD::from_shape_vec(vec![channel_count], vec![1.0e6; channel_count])
                .expect("width shape"),
        ));
        measurement_set
            .subtable_mut(SubtableId::SpectralWindow)
            .expect("SPECTRAL_WINDOW")
            .add_row(required_row(
                schema::spectral_window::REQUIRED_COLUMNS,
                &[
                    ("NUM_CHAN", int(channel_count as i32)),
                    ("NAME", string(&format!("CONTINUUM_{spw}"))),
                    ("REF_FREQUENCY", float(first_frequency_hz)),
                    ("TOTAL_BANDWIDTH", float(channel_count as f64 * 1.0e6)),
                    ("CHAN_FREQ", frequency),
                    ("CHAN_WIDTH", width.clone()),
                    ("EFFECTIVE_BW", width.clone()),
                    ("RESOLUTION", width),
                    ("MEAS_FREQ_REF", int(5)),
                    ("NET_SIDEBAND", int(1)),
                    ("FREQ_GROUP", int(0)),
                    ("FREQ_GROUP_NAME", string("")),
                    ("IF_CONV_CHAIN", int(0)),
                    ("FLAG_ROW", boolean(false)),
                ],
            ))
            .expect("add SPECTRAL_WINDOW row");
        measurement_set
            .subtable_mut(SubtableId::DataDescription)
            .expect("DATA_DESCRIPTION")
            .add_row(required_row(
                schema::data_description::REQUIRED_COLUMNS,
                &[
                    ("SPECTRAL_WINDOW_ID", int(spw as i32)),
                    ("POLARIZATION_ID", int(0)),
                    ("FLAG_ROW", boolean(false)),
                ],
            ))
            .expect("add DATA_DESCRIPTION row");
    }

    if options.native_evla_cf {
        for antenna in 0..antenna_count {
            measurement_set
                .subtable_mut(SubtableId::Feed)
                .expect("FEED")
                .add_row(required_row(
                    schema::feed::REQUIRED_COLUMNS,
                    &[
                        ("ANTENNA_ID", int(antenna as i32)),
                        ("FEED_ID", int(0)),
                        ("SPECTRAL_WINDOW_ID", int(-1)),
                        ("NUM_RECEPTORS", int(2)),
                        ("TIME", float(0.0)),
                        ("INTERVAL", float(0.0)),
                        (
                            "RECEPTOR_ANGLE",
                            Value::Array(ArrayValue::Float64(
                                ArrayD::from_shape_vec(vec![2], vec![0.0, 0.0]).unwrap(),
                            )),
                        ),
                    ],
                ))
                .unwrap();
        }
    }

    let visibilities = (0..correlation_count * channel_count)
        .map(|index| Complex32::new((index % 6 + 1) as f32, 0.0))
        .collect::<Vec<_>>();
    let flags = (0..correlation_count * channel_count)
        .map(|index| flag_cross_hand && index % 4 == 3)
        .collect::<Vec<_>>();
    let mut weights = vec![1.0; correlation_count];
    if let Some([first, last]) = parallel_hand_weights {
        weights[0] = first;
        weights[correlation_count - 1] = last;
    }
    let mut overrides = vec![
        ("ANTENNA1", int(0)),
        ("ANTENNA2", int(1)),
        ("FIELD_ID", int(0)),
        ("DATA_DESC_ID", int(0)),
        ("TIME", float(59_000.0 * 86_400.0)),
        ("TIME_CENTROID", float(59_000.0 * 86_400.0)),
        ("EXPOSURE", float(10.0)),
        ("INTERVAL", float(10.0)),
        ("SCAN_NUMBER", int(1)),
        (
            "UVW",
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(vec![3], vec![30.0, 40.0, 0.0]).expect("UVW shape"),
            )),
        ),
        (
            "DATA",
            Value::Array(ArrayValue::Complex32(
                ArrayD::from_shape_vec(vec![correlation_count, channel_count], visibilities)
                    .expect("DATA shape"),
            )),
        ),
        (
            "FLAG",
            Value::Array(ArrayValue::Bool(
                ArrayD::from_shape_vec(vec![correlation_count, channel_count], flags)
                    .expect("FLAG shape"),
            )),
        ),
        (
            "WEIGHT",
            Value::Array(ArrayValue::Float32(
                ArrayD::from_shape_vec(vec![correlation_count], weights.clone())
                    .expect("WEIGHT shape"),
            )),
        ),
        (
            "SIGMA",
            Value::Array(ArrayValue::Float32(
                ArrayD::from_shape_vec(vec![correlation_count], weights).expect("SIGMA shape"),
            )),
        ),
        ("FLAG_ROW", boolean(false)),
    ];
    if polarized {
        overrides.push((
            "MODEL_DATA",
            Value::Array(ArrayValue::Complex32(ArrayD::from_elem(
                vec![correlation_count, channel_count],
                Complex32::new(9.0, 9.0),
            ))),
        ));
    }
    if channel_count == 4 {
        overrides.push((
            "CORRECTED_DATA",
            Value::Array(ArrayValue::Complex32(
                ArrayD::from_shape_vec(
                    vec![correlation_count, channel_count],
                    (0..correlation_count * channel_count)
                        .map(|index| Complex32::new(20.0 + index as f32, -3.0))
                        .collect(),
                )
                .expect("CORRECTED_DATA shape"),
            )),
        ));
    }
    let baselines = (0..antenna_count)
        .flat_map(|first| ((first + 1)..antenna_count).map(move |second| (first, second)))
        .collect::<Vec<_>>();
    for row in 0..main_row_count {
        let (antenna1, antenna2) = baselines[row % baselines.len()];
        let integration = row / baselines.len();
        let mut row_overrides = overrides.clone();
        replace_override(&mut row_overrides, "ANTENNA1", int(antenna1 as i32));
        replace_override(&mut row_overrides, "ANTENNA2", int(antenna2 as i32));
        replace_override(
            &mut row_overrides,
            "DATA_DESC_ID",
            int((row % spectral_window_count) as i32),
        );
        replace_override(
            &mut row_overrides,
            "FIELD_ID",
            int((row % field_count) as i32),
        );
        replace_override(
            &mut row_overrides,
            "TIME",
            float(59_000.0 * 86_400.0 + 10.0 * integration as f64),
        );
        replace_override(
            &mut row_overrides,
            "TIME_CENTROID",
            float(59_000.0 * 86_400.0 + 10.0 * integration as f64),
        );
        replace_override(
            &mut row_overrides,
            "UVW",
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(
                    vec![3],
                    vec![
                        30.0 * (antenna2 - antenna1) as f64,
                        20.0 * (antenna1 + antenna2 + 1) as f64,
                        2.0 * integration as f64,
                    ],
                )
                .expect("UVW shape"),
            )),
        );
        add_main_row(measurement_set, &row_overrides);
    }
}

pub(super) fn replace_override(overrides: &mut [(&str, Value)], name: &str, value: Value) {
    overrides
        .iter_mut()
        .find(|(candidate, _)| *candidate == name)
        .expect("fixture override")
        .1 = value;
}

pub(super) fn add_main_row(measurement_set: &mut MeasurementSet, overrides: &[(&str, Value)]) {
    let schema = measurement_set
        .main_table()
        .schema()
        .expect("MAIN schema")
        .clone();
    let definitions = schema::main_table::REQUIRED_COLUMNS
        .iter()
        .chain(schema::main_table::OPTIONAL_COLUMNS.iter())
        .collect::<Vec<_>>();
    let fields = schema
        .columns()
        .iter()
        .map(|column| {
            overrides
                .iter()
                .find(|(name, _)| *name == column.name())
                .map(|(_, value)| RecordField::new(column.name(), value.clone()))
                .unwrap_or_else(|| {
                    let definition = definitions
                        .iter()
                        .find(|definition| definition.name == column.name())
                        .expect("standard MAIN column");
                    RecordField::new(column.name(), default_value(definition))
                })
        })
        .collect();
    measurement_set
        .main_table_mut()
        .add_row(RecordValue::new(fields))
        .expect("add MAIN row");
}

pub(super) fn required_row(definitions: &[ColumnDef], overrides: &[(&str, Value)]) -> RecordValue {
    RecordValue::new(
        definitions
            .iter()
            .map(|definition| {
                overrides
                    .iter()
                    .find(|(name, _)| *name == definition.name)
                    .map(|(_, value)| RecordField::new(definition.name, value.clone()))
                    .unwrap_or_else(|| RecordField::new(definition.name, default_value(definition)))
            })
            .collect(),
    )
}

pub(super) fn default_value(definition: &ColumnDef) -> Value {
    match definition.column_kind {
        ColumnKind::Scalar => match definition.data_type {
            PrimitiveType::Int32 => int(0),
            PrimitiveType::Float64 => float(0.0),
            PrimitiveType::Bool => boolean(false),
            PrimitiveType::String => string(""),
            other => panic!("unsupported fixture scalar type {other:?}"),
        },
        ColumnKind::FixedArray { shape } => Value::Array(ArrayValue::Float64(
            ArrayD::from_shape_vec(shape.to_vec(), vec![0.0; shape.iter().product()])
                .expect("fixed-array default shape"),
        )),
        ColumnKind::VariableArray { ndim } => {
            let shape = vec![1; ndim];
            match definition.data_type {
                PrimitiveType::Bool => Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(shape, vec![false]).expect("bool default shape"),
                )),
                PrimitiveType::Float32 => Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(shape, vec![0.0]).expect("f32 default shape"),
                )),
                PrimitiveType::Float64 => Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(shape, vec![0.0]).expect("f64 default shape"),
                )),
                PrimitiveType::Int32 => Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(shape, vec![0]).expect("i32 default shape"),
                )),
                PrimitiveType::String => Value::Array(ArrayValue::String(
                    ArrayD::from_shape_vec(shape, vec![String::new()])
                        .expect("string default shape"),
                )),
                PrimitiveType::Complex32 => Value::Array(ArrayValue::Complex32(
                    ArrayD::from_shape_vec(shape, vec![Complex32::new(0.0, 0.0)])
                        .expect("complex default shape"),
                )),
                other => panic!("unsupported fixture array type {other:?}"),
            }
        }
    }
}

pub(super) fn int(value: i32) -> Value {
    Value::Scalar(ScalarValue::Int32(value))
}

pub(super) fn float(value: f64) -> Value {
    Value::Scalar(ScalarValue::Float64(value))
}

pub(super) fn boolean(value: bool) -> Value {
    Value::Scalar(ScalarValue::Bool(value))
}

pub(super) fn string(value: &str) -> Value {
    Value::Scalar(ScalarValue::String(value.to_string()))
}

pub(super) fn write_aw_test_cache(root: &Path) {
    std::fs::create_dir(root).expect("create AW cache root");
    for (frequency_suffix, frequency_hz) in [("40ghz", 40.0e9), ("48ghz", 48.0e9)] {
        for (polarization_suffix, mueller) in [("rr", 0), ("ll", 15)] {
            let suffix = format!("{polarization_suffix}_{frequency_suffix}");
            write_aw_test_cell(
                root,
                &format!("CFS_{suffix}.im"),
                false,
                [-2.0, 2.0],
                mueller,
                frequency_hz,
                Complex32::new(3.0, -1.0),
            );
            write_aw_test_cell(
                root,
                &format!("WTCFS_{suffix}.im"),
                true,
                [-1.0, 1.0],
                mueller,
                frequency_hz,
                Complex32::new(7.0, 2.0),
            );
        }
    }
}

pub(super) fn aw_projection(casa_cache: PathBuf, use_pointing: bool) -> ContinuumAwProjection {
    ContinuumAwProjection {
        source: casa_imaging_application::ContinuumAwCfSource::CasaImport(casa_cache),
        resident_bytes: 1 << 20,
        w_plane_count: Some(32),
        psf_phase_center_direction_rad: None,
        vp_table: None,
        a_term: true,
        ps_term: false,
        wideband: true,
        conjugate_beams: true,
        use_pointing,
        pointing_offset_sigdev: Vec::new(),
        mosaic_weighting: false,
        compute_pa_step_deg: 360.0,
        rotate_pa_step_deg: 360.0,
    }
}

pub(super) fn write_aw_test_cell(
    root: &Path,
    name: &str,
    weight: bool,
    increment: [f64; 2],
    mueller: i32,
    frequency_hz: f64,
    value: Complex32,
) {
    write_aw_sized_test_cell(
        root,
        name,
        weight,
        increment,
        mueller,
        frequency_hz,
        value,
        if weight { 32 } else { 16 },
    );
}

#[allow(clippy::too_many_arguments)]
pub(super) fn write_aw_sized_test_cell(
    root: &Path,
    name: &str,
    weight: bool,
    increment: [f64; 2],
    mueller: i32,
    frequency_hz: f64,
    value: Complex32,
    extent: usize,
) {
    let path = root.join(name);
    let shape = vec![extent, extent, 1, 1];
    let reference_pixel = vec![(extent / 2) as f64, (extent / 2) as f64];
    let support = if weight { 2 } else { 1 };
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(
        LinearCoordinate::new(
            2,
            vec!["UU".to_string(), "VV".to_string()],
            vec!["lambda".to_string(), "lambda".to_string()],
        )
        .with_reference_value(vec![0.0, 0.0])
        .with_reference_pixel(reference_pixel)
        .with_increment(increment.to_vec()),
    );
    coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::RR]));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        frequency_hz,
        1.0,
        0.0,
        frequency_hz,
    ));
    let mut image =
        PagedImage::<Complex32>::create(shape, coordinates, &path).expect("create AW cache cell");
    image.set(value).expect("fill AW cache cell");
    image
        .set_misc_info(RecordValue::new(vec![
            RecordField::new(
                "BandName",
                Value::Scalar(ScalarValue::String("EVLA_Q".to_string())),
            ),
            RecordField::new(
                "ConjFreq",
                Value::Scalar(ScalarValue::Float64(frequency_hz)),
            ),
            RecordField::new("ConjPoln", Value::Scalar(ScalarValue::Int32(8))),
            RecordField::new("Diameter", Value::Scalar(ScalarValue::Float64(25.0))),
            RecordField::new("MuellerElement", Value::Scalar(ScalarValue::Int32(mueller))),
            RecordField::new("OpCode", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new(
                "ParallacticAngle",
                Value::Scalar(ScalarValue::Float64(30.0)),
            ),
            RecordField::new("Sampling", Value::Scalar(ScalarValue::Float64(2.0))),
            RecordField::new(
                "TelescopeName",
                Value::Scalar(ScalarValue::String("EVLA".to_string())),
            ),
            RecordField::new("WIncr", Value::Scalar(ScalarValue::Float64(0.5))),
            RecordField::new("WValue", Value::Scalar(ScalarValue::Float64(0.0))),
            RecordField::new("Xsupport", Value::Scalar(ScalarValue::Int32(support))),
            RecordField::new("Ysupport", Value::Scalar(ScalarValue::Int32(support))),
        ]))
        .expect("attach AW cache metadata");
    image.save().expect("save AW cache cell");
}

pub(super) fn request(
    measurement_set: PathBuf,
    image_name: PathBuf,
    algorithm: ContinuumAlgorithm,
) -> ContinuumImagingRequest {
    ContinuumImagingRequest {
        measurement_set,
        image_name,
        image_size: 16,
        facets: 1,
        cell_arcsec: 1.0,
        phase_center_field: None,
        phase_center: None,
        outlier_file: None,
        field_ids: Some(vec![0]),
        uv_range: None,
        intent: None,
        data_description: Some(0),
        spectral_window: None,
        channel_start: Some(0),
        channel_count: Some(1),
        spectral_mode: SpectralImagingMode::Continuum,
        continuum_subtraction: None,
        data_column: Some("DATA".to_string()),
        polarizations: vec![casa_imaging_application::PolarizationCoordinate::StokesI],
        algorithm,
        weighting: ContinuumWeighting::Natural,
        iterations: 1,
        cycle_iterations: 1,
        hogbom_iteration_accounting: casa_imaging_application::HogbomIterationAccounting::Strict,
        maximum_major_cycles: Some(1),
        noise_sigma: None,
        cycle_factor: 1.0,
        minimum_psf_fraction: 0.05,
        maximum_psf_fraction: 0.8,
        gain: 1.0,
        threshold_jy: 0.0,
        psf_cutoff: 0.2,
        primary_beam_cutoff: 0.2,
        normalization: casa_imaging_model::ProductNormalization::UnitResponse,
        beam_policy: ContinuumBeamPolicy::PerPlane,
        mask: ContinuumMask::FullPlane,
        save_model_column: false,
        save_continuum_residual: false,
        write_primary_beam: false,
        pbcor: false,
        w_projection_planes: None,
        aw_projection: None,
        task_requirements: Vec::new(),
        resource_policy: casa_imaging_runtime::ResourcePolicy::Balanced,
    }
}

pub(super) fn set_production_io_environment() {
    // The application deliberately requires measured spill rates at its
    // production boundary; these values are only test calibration facts.
    unsafe {
        std::env::set_var("CASA_RS_IMAGING_SPILL_READ_BYTES_PER_SECOND", "1000000000");
        std::env::set_var("CASA_RS_IMAGING_SPILL_WRITE_BYTES_PER_SECOND", "1000000000");
    }
}
