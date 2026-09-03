// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

use casa_coordinates::{
    CoordinateModel, CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, StokesType,
};
use casa_images::PagedImage;
use casa_imaging_application::{
    ContinuumAlgorithm, ContinuumAutoMaskControls, ContinuumBeamPolicy, ContinuumImagingRequest,
    ContinuumMask, ContinuumMaskBox, ContinuumStopReason, ContinuumWeighting, SpectralImagingMode,
    TaskRequirement, VisibilityContinuumSubtraction, execute_continuum,
};
use casa_imaging_model::ImageDomainRole;
use casa_ms::{
    CubeAxisConfig, CubeAxisValue, MeasurementSet, MeasurementSetBuilder, OptionalMainColumn,
    SubtableId, VisibilityDataColumn,
    column_def::{ColumnDef, ColumnKind},
    initialize_measurement_set_owner_manifest, schema,
};
use casa_types::{
    ArrayValue, Complex32, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
    measures::frequency::FrequencyRef,
};
use ndarray::ArrayD;

const PRODUCT_SUFFIXES: [&str; 6] = [".psf", ".residual", ".model", ".image", ".sumwt", ".mask"];
const DIRTY_PRODUCT_SUFFIXES: [&str; 5] = [".psf", ".residual", ".model", ".image", ".sumwt"];

static EXECUTION_LOCK: Mutex<()> = Mutex::new(());

fn tiny_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "input.ms",
        MeasurementSetFixtureOptions::new(false, false, 1, 1, 2, 1, false),
    )
}

fn multi_row_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "multi-row-input.ms",
        MeasurementSetFixtureOptions::new(false, false, 1, 1, 2, 8, false),
    )
}

fn flagged_polarized_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "polarized-input.ms",
        MeasurementSetFixtureOptions::new(true, true, 2, 1, 2, 1, false),
    )
}

fn full_stokes_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "full-stokes-input.ms",
        MeasurementSetFixtureOptions::new(true, false, 2, 1, 27, 702, false),
    )
}

fn unequal_linear_parallel_hand_measurement_set(
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

fn spectral_line_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "line-input.ms",
        MeasurementSetFixtureOptions::new(true, true, 4, 1, 2, 1, false),
    )
}

fn thirty_two_channel_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "thirty-two-channel-input.ms",
        MeasurementSetFixtureOptions::new(false, false, 32, 1, 2, 1, false),
    )
}

fn thirty_two_channel_multi_row_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "thirty-two-channel-multi-row-input.ms",
        MeasurementSetFixtureOptions::new(false, false, 32, 1, 2, 8, false).with_two_fields(),
    )
}

fn joint_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "joint-input.ms",
        MeasurementSetFixtureOptions::new(false, false, 4, 1, 2, 1, false),
    )
}

fn undefined_weight_spectrum_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "undefined-weight-spectrum.ms",
        MeasurementSetFixtureOptions::new(false, false, 1, 1, 2, 1, true),
    )
}

fn four_spw_aca_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(
        root,
        "four-spw-input.ms",
        MeasurementSetFixtureOptions::new(false, false, 8, 4, 4, 24, false)
            .with_aca_observation_metadata(),
    )
}

#[derive(Clone, Copy)]
struct MeasurementSetFixtureOptions {
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

    const fn with_two_fields(mut self) -> Self {
        self.field_count = 2;
        self
    }
}

fn measurement_set_fixture(
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

fn populate_fixture(measurement_set: &mut MeasurementSet, options: MeasurementSetFixtureOptions) {
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
        let first_frequency_hz = 44.0e9 + spw as f64 * 100.0e6;
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

fn replace_override(overrides: &mut [(&str, Value)], name: &str, value: Value) {
    overrides
        .iter_mut()
        .find(|(candidate, _)| *candidate == name)
        .expect("fixture override")
        .1 = value;
}

fn add_main_row(measurement_set: &mut MeasurementSet, overrides: &[(&str, Value)]) {
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

fn required_row(definitions: &[ColumnDef], overrides: &[(&str, Value)]) -> RecordValue {
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

fn default_value(definition: &ColumnDef) -> Value {
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

fn int(value: i32) -> Value {
    Value::Scalar(ScalarValue::Int32(value))
}

fn float(value: f64) -> Value {
    Value::Scalar(ScalarValue::Float64(value))
}

fn boolean(value: bool) -> Value {
    Value::Scalar(ScalarValue::Bool(value))
}

fn string(value: &str) -> Value {
    Value::Scalar(ScalarValue::String(value.to_string()))
}

fn request(
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

fn set_production_io_environment() {
    // The application deliberately requires measured spill rates at its
    // production boundary; these values are only test calibration facts.
    unsafe {
        std::env::set_var("CASA_RS_IMAGING_SPILL_READ_BYTES_PER_SECOND", "1000000000");
        std::env::set_var("CASA_RS_IMAGING_SPILL_WRITE_BYTES_PER_SECOND", "1000000000");
    }
}

fn assert_standard_products(image_name: &Path, product_names: &[String]) {
    assert_products(image_name, product_names, &PRODUCT_SUFFIXES);
}

fn assert_dirty_products(image_name: &Path, product_names: &[String]) {
    assert_products(image_name, product_names, &DIRTY_PRODUCT_SUFFIXES);
}

fn assert_products(image_name: &Path, product_names: &[String], suffixes: &[&str]) {
    let expected = suffixes
        .iter()
        .map(|suffix| (*suffix).to_string())
        .collect::<Vec<_>>();
    assert_eq!(product_names, expected);
    for suffix in suffixes {
        let path = PathBuf::from(format!("{}{}", image_name.display(), suffix));
        assert!(
            path.is_dir(),
            "missing CASA product directory {}",
            path.display()
        );
    }
}

fn product_plane(image_name: &Path, suffix: &str) -> ArrayD<f32> {
    product_plane_with_size(image_name, suffix, 16)
}

fn product_plane_with_size(image_name: &Path, suffix: &str, image_size: usize) -> ArrayD<f32> {
    PagedImage::<f32>::open(PathBuf::from(format!("{}{}", image_name.display(), suffix)))
        .expect("open application product")
        .get_slice(&[0, 0, 0, 0], &[image_size, image_size, 1, 1])
        .expect("read application product plane")
}

fn assert_model_residual_respect_mask(image_name: &Path, expected_mask_pixels: usize) {
    let mask = product_plane(image_name, ".mask");
    let model = product_plane(image_name, ".model");
    let residual = product_plane(image_name, ".residual");
    assert_eq!(
        mask.iter().filter(|value| **value != 0.0).count(),
        expected_mask_pixels
    );
    assert!(
        model
            .iter()
            .zip(mask.iter())
            .all(|(model, mask)| *mask != 0.0 || *model == 0.0),
        "the model must remain zero outside the reconstruction mask"
    );
    assert!(residual.iter().all(|value| value.is_finite()));
}

#[test]
fn application_executes_single_ddid_stokes_i_mfs_dirty_and_publishes_products() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("dirty");

    let result = execute_continuum(request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Dirty,
    ))
    .expect("native dirty application execution");

    assert_eq!(result.minor_iterations, 0);
    assert_eq!(result.minor_stop_reason, None);
    assert_dirty_products(&image_name, &result.product_names);
    for suffix in [".residual", ".image"] {
        let product =
            PagedImage::<f32>::open(PathBuf::from(format!("{}{}", image_name.display(), suffix)))
                .expect("reopen validity-bearing product");
        assert_eq!(product.default_mask_name(), None);
    }
    assert!(!PathBuf::from(format!("{}.mask", image_name.display())).exists());
}

#[test]
fn t49_application_executes_nonzero_w_through_major_cycle_replay() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = multi_row_measurement_set(root.path());
    let image_name = root.path().join("w-projection");
    let mut imaging = request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Hogbom,
    );
    imaging.image_size = 32;
    imaging.iterations = 2;
    imaging.cycle_iterations = 1;
    imaging.maximum_major_cycles = Some(2);
    imaging.w_projection_planes = Some(5);
    imaging.task_requirements = vec![
        TaskRequirement::WProjection,
        TaskRequirement::WProjectionPlanes,
    ];

    let result = execute_continuum(imaging).expect("native W-projection execution");
    assert_eq!(result.minor_iterations, 2);
    assert_standard_products(&image_name, &result.product_names);
}

#[test]
fn t49_plane_count_does_not_infer_w_projection() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("w-planes-without-capability");
    let mut imaging = request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Dirty,
    );
    imaging.w_projection_planes = Some(5);

    let error = match execute_continuum(imaging) {
        Ok(_) => panic!("plane count inferred W projection"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("requires the explicit W-projection task capability"),
        "wrong explicit-W error: {error}"
    );
    assert!(!PathBuf::from(format!("{}.psf", image_name.display())).exists());
}

#[test]
fn t49_zero_projected_w_matches_the_production_standard_operator() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let standard_name = root.path().join("zero-w-standard");
    let w_name = root.path().join("zero-w-requested");

    execute_continuum(request(
        measurement_set.clone(),
        standard_name.clone(),
        ContinuumAlgorithm::Dirty,
    ))
    .expect("standard zero-W execution");
    let mut w_request = request(measurement_set, w_name.clone(), ContinuumAlgorithm::Dirty);
    w_request.task_requirements = vec![TaskRequirement::WProjection];
    execute_continuum(w_request).expect("requested zero-W execution");

    for suffix in DIRTY_PRODUCT_SUFFIXES {
        assert_eq!(
            product_plane(&standard_name, suffix),
            product_plane(&w_name, suffix),
            "zero projected W must reduce structurally to Standard for {suffix}",
        );
    }
}

#[test]
fn t49_w_projection_composes_with_multifield_recentered_cube() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = thirty_two_channel_multi_row_measurement_set(root.path());
    let image_name = root.path().join("w-cube-main");
    let outlier_name = root.path().join("w-cube-outlier");
    let outlier_file = root.path().join("w-cube.outlier");
    std::fs::write(
        &outlier_file,
        format!(
            "imagename={}\nimsize=[32,32]\ncell=[1arcsec,1arcsec]\nphasecenter=J2000 1.001rad 0.499rad\nmask=circle[[16pix,16pix],8pix]\n",
            outlier_name.display()
        ),
    )
    .expect("write recentered W-cube outlier");
    let mut imaging = request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Dirty,
    );
    imaging.image_size = 32;
    imaging.field_ids = Some(vec![0, 1]);
    imaging.outlier_file = Some(outlier_file);
    imaging.spectral_window = Some("0:0~31".to_string());
    imaging.channel_count = Some(32);
    imaging.spectral_mode = SpectralImagingMode::Cube {
        axis: CubeAxisConfig {
            outframe: FrequencyRef::TOPO,
            ..CubeAxisConfig::default()
        },
        output_channels: Some(32),
    };
    imaging.w_projection_planes = Some(5);
    imaging.task_requirements = vec![
        TaskRequirement::SpectralCube,
        TaskRequirement::WProjection,
        TaskRequirement::WProjectionPlanes,
    ];

    let result = execute_continuum(imaging)
        .expect("native recentered, faceted, multi-domain W-cube execution");
    assert_eq!(
        result
            .outcome
            .output
            .scientific
            .normal_state()
            .domain_count(),
        2
    );
    assert_eq!(result.outcome.output.planned_products.members().len(), 10);
    for base in [&image_name, &outlier_name] {
        for suffix in DIRTY_PRODUCT_SUFFIXES {
            let product =
                PagedImage::<f32>::open(PathBuf::from(format!("{}{suffix}", base.display())))
                    .expect("open W-cube product");
            assert_eq!(
                product.shape(),
                if suffix == ".sumwt" {
                    &[1, 1, 1, 32]
                } else {
                    &[32, 32, 1, 32]
                },
                "{suffix}"
            );
        }
    }
}

#[test]
fn t49_w_projection_composes_with_faceted_continuum() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = multi_row_measurement_set(root.path());
    let image_name = root.path().join("w-faceted-continuum");
    let mut imaging = request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Dirty,
    );
    imaging.image_size = 32;
    imaging.facets = 2;
    imaging.w_projection_planes = Some(5);
    imaging.task_requirements = vec![
        TaskRequirement::WProjection,
        TaskRequirement::WProjectionPlanes,
    ];

    let result =
        execute_continuum(imaging).expect("native faceted W-projection continuum execution");
    assert_eq!(
        result
            .outcome
            .output
            .scientific
            .normal_state()
            .domain_count(),
        1
    );
    assert_dirty_products(&image_name, &result.product_names);
}

#[test]
fn stokes_i_uses_one_shared_imaging_weight_for_each_linear_parallel_hand() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");

    let run = |name: &str, weights: [f32; 2]| {
        let measurement_set =
            unequal_linear_parallel_hand_measurement_set(root.path(), name, weights);
        let image_name = root.path().join(format!("{name}-dirty"));
        execute_continuum(request(
            measurement_set,
            image_name.clone(),
            ContinuumAlgorithm::Dirty,
        ))
        .expect("native unequal-XX/YY dirty execution");
        let psf = product_plane(&image_name, ".psf");
        let residual = product_plane(&image_name, ".residual");
        let sumwt =
            PagedImage::<f32>::open(PathBuf::from(format!("{}.sumwt", image_name.display())))
                .expect("open Stokes-I sum weights")
                .get()
                .expect("read Stokes-I sum weights");
        (psf, residual, sumwt)
    };

    let (equal_psf, equal_residual, equal_sumwt) = run("equal-hands", [2.0, 2.0]);
    let (unequal_psf, unequal_residual, unequal_sumwt) = run("unequal-hands", [1.0, 3.0]);

    assert_eq!(equal_psf, unequal_psf, "the common mean preserves the PSF");
    assert!(
        equal_residual.iter().any(|value| value.abs() > 0.0),
        "the numerator comparison must be non-vacuous"
    );
    for (equal, unequal) in equal_residual.iter().zip(unequal_residual.iter()) {
        assert!(
            (equal - unequal).abs() <= 1.0e-6,
            "shared per-hand weighting changed the Stokes-I numerator: equal={equal} unequal={unequal}"
        );
    }
    assert_eq!(
        equal_sumwt.as_slice().expect("contiguous sum weights"),
        &[4.0]
    );
    assert_eq!(
        unequal_sumwt.as_slice().expect("contiguous sum weights"),
        &[4.0],
        "both mapped hands contribute their shared row/channel imaging weight"
    );
}

#[test]
fn application_executes_full_stokes_mfs_clean_with_complete_products_and_axes() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = full_stokes_measurement_set(root.path());
    let image_name = root.path().join("full-stokes-dirty");
    let mut imaging = request(
        measurement_set.clone(),
        image_name.clone(),
        ContinuumAlgorithm::Hogbom,
    );
    imaging.polarizations = vec![
        casa_imaging_application::PolarizationCoordinate::StokesI,
        casa_imaging_application::PolarizationCoordinate::StokesQ,
        casa_imaging_application::PolarizationCoordinate::StokesU,
        casa_imaging_application::PolarizationCoordinate::StokesV,
    ];
    imaging.image_size = 64;
    imaging.iterations = 4;
    imaging.cycle_iterations = 4;
    imaging.save_model_column = true;
    imaging.task_requirements = vec![
        TaskRequirement::PolarizationSelection,
        TaskRequirement::ModelColumnWrite,
    ];

    let result = execute_continuum(imaging).expect("native full-Stokes Högbom execution");
    assert_eq!(result.minor_iterations, 1);
    assert_eq!(result.actual_minor_iterations, 1);
    assert_eq!(
        result.minor_stop_reason,
        Some(ContinuumStopReason::ThresholdReached),
        "an early scientific stop reports the actual component count without CASA iteration-bound clamping"
    );
    assert_eq!(
        result
            .outcome
            .output
            .visibility_products
            .as_ref()
            .expect("full-Stokes visibility completion")
            .sample_count(),
        2_808
    );
    assert_standard_products(&image_name, &result.product_names);
    for suffix in [".psf", ".residual", ".model", ".image"] {
        let product =
            PagedImage::<f32>::open(PathBuf::from(format!("{}{suffix}", image_name.display())))
                .expect("reopen full-Stokes product");
        assert_eq!(product.shape(), &[64, 64, 4, 1]);
        let CoordinateModel::Stokes(stokes) = product.coordinates().coordinate(1) else {
            panic!("full-Stokes product has no polarization coordinate")
        };
        assert_eq!(
            stokes.stokes(),
            &[StokesType::I, StokesType::Q, StokesType::U, StokesType::V]
        );
        assert_eq!(
            product.units(),
            if suffix == ".model" {
                "Jy/pixel"
            } else {
                "Jy/beam"
            }
        );
        assert!(
            product
                .get()
                .expect("read full-Stokes payload")
                .iter()
                .all(|value| value.is_finite())
        );
    }
    let sum_weights =
        PagedImage::<f32>::open(PathBuf::from(format!("{}.sumwt", image_name.display())))
            .expect("reopen full-Stokes sum weights");
    assert_eq!(sum_weights.shape(), &[1, 1, 4, 1]);
    assert_eq!(
        sum_weights.get().expect("read full-Stokes sum weights"),
        ArrayD::from_shape_vec(vec![1, 1, 4, 1], vec![452.0; 4])
            .expect("full-Stokes sum-weight shape")
    );
    let reopened = MeasurementSet::open(&measurement_set).expect("reopen full-Stokes MODEL_DATA");
    let model = reopened
        .data_column(VisibilityDataColumn::ModelData)
        .expect("full-Stokes MODEL_DATA");
    let ArrayValue::Complex32(model) = model.get(0).expect("full-Stokes MODEL_DATA row") else {
        panic!("full-Stokes MODEL_DATA is complex")
    };
    assert!(
        model
            .iter()
            .all(|value| value.re.is_finite() && value.im.is_finite())
    );
    assert!(model.iter().any(|value| *value != Complex32::new(9.0, 9.0)));
}

#[test]
fn application_executes_raw_linear_correlation_products_with_exact_axis() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = full_stokes_measurement_set(root.path());
    let image_name = root.path().join("linear-correlations");
    let mut imaging = request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Dirty,
    );
    imaging.polarizations = vec![
        casa_imaging_application::PolarizationCoordinate::LinearXx,
        casa_imaging_application::PolarizationCoordinate::LinearXy,
        casa_imaging_application::PolarizationCoordinate::LinearYx,
        casa_imaging_application::PolarizationCoordinate::LinearYy,
    ];
    imaging.image_size = 64;
    imaging.task_requirements = vec![TaskRequirement::PolarizationSelection];

    let result = execute_continuum(imaging).expect("native raw-correlation dirty execution");
    assert_dirty_products(&image_name, &result.product_names);
    let product =
        PagedImage::<f32>::open(PathBuf::from(format!("{}.residual", image_name.display())))
            .expect("reopen raw-correlation residual");
    assert_eq!(product.shape(), &[64, 64, 4, 1]);
    let CoordinateModel::Stokes(stokes) = product.coordinates().coordinate(1) else {
        panic!("raw-correlation product has no polarization coordinate")
    };
    assert_eq!(
        stokes.stokes(),
        &[
            StokesType::XX,
            StokesType::XY,
            StokesType::YX,
            StokesType::YY
        ]
    );
    assert_eq!(product.units(), "Jy/beam");
}

#[test]
fn application_uses_weight_when_selected_weight_spectrum_cells_are_undefined() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = undefined_weight_spectrum_measurement_set(root.path());
    let image_name = root.path().join("undefined-weight-spectrum-dirty");

    let result = execute_continuum(request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Dirty,
    ))
    .expect("undefined WEIGHT_SPECTRUM cells select scalar WEIGHT before traversal");

    assert_dirty_products(&image_name, &result.product_names);
}

#[test]
fn t31_application_executes_recentered_domains_through_one_scientific_route() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());

    for (label, algorithm) in [
        ("dirty", ContinuumAlgorithm::Dirty),
        ("hogbom", ContinuumAlgorithm::Hogbom),
    ] {
        let product_suffixes = if algorithm == ContinuumAlgorithm::Dirty {
            DIRTY_PRODUCT_SUFFIXES.as_slice()
        } else {
            PRODUCT_SUFFIXES.as_slice()
        };
        let image_name = root.path().join(format!("t31-{label}-main"));
        let outlier_name = root.path().join(format!("t31-{label}-outlier"));
        let outlier_file = root.path().join(format!("t31-{label}.outlier"));
        std::fs::write(
            &outlier_file,
            format!(
                "imagename={}\nimsize=[16,16]\ncell=[1arcsec,1arcsec]\nphasecenter=J2000 1.001rad 0.499rad\nusemask=user\nmask=circle[[8pix,8pix],4pix]\nspecmode=mfs\nnchan=1\nnterms=1\ngridder=standard\ndeconvolver=hogbom\nwprojplanes=1\n",
                outlier_name.display()
            ),
        )
        .expect("write CASA outlier fixture");
        let mut imaging = request(measurement_set.clone(), image_name.clone(), algorithm);
        imaging.outlier_file = Some(outlier_file);
        imaging.task_requirements = vec![TaskRequirement::SerialCpu, TaskRequirement::FixedTileCpu];

        let result = execute_continuum(imaging).expect("execute T31 multi-domain application");
        assert_eq!(
            result
                .outcome
                .output
                .scientific
                .normal_state()
                .domain_count(),
            2
        );
        assert_eq!(
            result.outcome.output.planned_products.members().len(),
            2 * product_suffixes.len()
        );
        assert_eq!(
            result
                .outcome
                .output
                .planned_products
                .members()
                .iter()
                .filter(|member| member.axes().domain() == &ImageDomainRole::Main)
                .count(),
            product_suffixes.len()
        );
        assert_eq!(
            result
                .outcome
                .output
                .planned_products
                .members()
                .iter()
                .filter(|member| {
                    member.axes().domain()
                        == &ImageDomainRole::Outlier(outlier_name.display().to_string())
                })
                .count(),
            product_suffixes.len()
        );

        for (base, expected) in [(&image_name, [1.0, 0.5]), (&outlier_name, [1.001, 0.499])] {
            for suffix in product_suffixes {
                assert!(
                    PathBuf::from(format!("{}{suffix}", base.display())).is_dir(),
                    "missing {label} domain product {}{suffix}",
                    base.display()
                );
            }
            let psf = PagedImage::<f32>::open(PathBuf::from(format!("{}.psf", base.display())))
                .expect("open domain PSF");
            let world = psf
                .coordinates()
                .to_world(&[8.0, 8.0, 0.0, 0.0])
                .expect("domain reference world coordinate");
            assert!((world[0] - expected[0]).abs() < 1.0e-12);
            assert!((world[1] - expected[1]).abs() < 1.0e-12);
            for suffix in [".psf", ".residual", ".model", ".image"] {
                assert!(
                    product_plane(base, suffix)
                        .iter()
                        .all(|value| value.is_finite()),
                    "{label} {suffix} must be finite on valid support"
                );
            }
        }

        let model_nonzero = result
            .outcome
            .output
            .scientific
            .final_model()
            .samples()
            .iter()
            .filter(|sample| sample.value().value() != 0.0)
            .count();
        match label {
            "dirty" => assert_eq!(model_nonzero, 0),
            "hogbom" => assert_eq!(model_nonzero, 2),
            _ => unreachable!(),
        }
    }
}

#[test]
fn t31_application_canonicalizes_reversed_outliers_before_domain_indexed_derivations() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let main = root.path().join("main");
    let alpha = root.path().join("alpha");
    let zeta = root.path().join("zeta");
    let outlier_file = root.path().join("reversed.outlier");
    std::fs::write(
        &outlier_file,
        "imagename=zeta\nimsize=[10,10]\ncell=[1arcsec,1arcsec]\nphasecenter=J2000 1.003rad 0.497rad\nusemask=user\nmask=circle[[5pix,5pix],2pix]\n\
         imagename=alpha\nimsize=[12,12]\ncell=[1arcsec,1arcsec]\nphasecenter=J2000 0.998rad 0.502rad\nusemask=user\nmask=circle[[3pix,3pix],1pix]\n",
    )
    .expect("write reversed CASA outlier fixture");
    let mut imaging = request(measurement_set, main.clone(), ContinuumAlgorithm::Hogbom);
    imaging.outlier_file = Some(outlier_file);
    imaging.task_requirements = vec![TaskRequirement::SerialCpu, TaskRequirement::FixedTileCpu];

    let result = execute_continuum(imaging).expect("execute canonical multi-domain application");
    let expected = [
        (ImageDomainRole::Main, main.as_path(), 16, [1.0, 0.5], 256),
        (
            ImageDomainRole::Outlier("alpha".to_string()),
            alpha.as_path(),
            12,
            [0.998, 0.502],
            5,
        ),
        (
            ImageDomainRole::Outlier("zeta".to_string()),
            zeta.as_path(),
            10,
            [1.003, 0.497],
            13,
        ),
    ];

    let normal_roles = result
        .outcome
        .output
        .scientific
        .normal_state()
        .domains()
        .map(|domain| domain.role().clone())
        .collect::<Vec<_>>();
    assert_eq!(
        normal_roles,
        expected
            .iter()
            .map(|(role, ..)| role.clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        result.outcome.output.planned_products.generation_id(),
        result.outcome.output.products.generation_id(),
        "publication must retain the exact canonical domain generation"
    );
    for (planned, published) in result
        .outcome
        .output
        .planned_products
        .members()
        .iter()
        .zip(result.outcome.output.products.members())
    {
        assert_eq!(planned.artifact_id(), published.artifact_id());
        assert_eq!(
            planned.axes().domain(),
            published.contract().axes().domain()
        );
    }

    for (role, base, image_size, direction, expected_mask_pixels) in expected {
        let members = result
            .outcome
            .output
            .planned_products
            .members()
            .iter()
            .filter(|member| member.axes().domain() == &role)
            .collect::<Vec<_>>();
        assert_eq!(members.len(), 6, "wrong product association for {role:?}");
        assert!(members.iter().any(|member| member.name() == ".mask"));
        assert!(
            members
                .iter()
                .filter(|member| member.name() != ".sumwt")
                .all(|member| member.shape()[..2] == [image_size, image_size])
        );

        let psf = PagedImage::<f32>::open(PathBuf::from(format!("{}.psf", base.display())))
            .expect("open domain PSF");
        let reference = image_size as f64 / 2.0;
        let world = psf
            .coordinates()
            .to_world(&[reference, reference, 0.0, 0.0])
            .expect("domain reference world coordinate");
        assert!((world[0] - direction[0]).abs() < 1.0e-12);
        assert!((world[1] - direction[1]).abs() < 1.0e-12);

        let mask = product_plane_with_size(base, ".mask", image_size);
        assert_eq!(
            mask.iter().filter(|value| **value != 0.0).count(),
            expected_mask_pixels,
            "wrong mask support published for {role:?}"
        );
    }
}

#[test]
fn optional_joint_application_route_fails_closed_before_execution() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = joint_measurement_set(root.path());
    let image_name = root.path().join("joint-continuum-line");
    let mut imaging = request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::JointContinuumLine {
            continuum_terms: 1,
            continuum_anchor_channels: vec![0, 1],
            line_channels: vec![2, 3],
            maximum_condition_number: 1.0e12,
            scales_px: vec![0.0],
            small_scale_bias: 0.0,
        },
    );
    imaging.spectral_window = Some("0:0~3".to_string());
    imaging.channel_count = Some(4);
    imaging.spectral_mode = SpectralImagingMode::JointContinuumLine;
    imaging.beam_policy = ContinuumBeamPolicy::Common;
    imaging.mask = ContinuumMask::Coupled {
        continuum: Box::new(ContinuumMask::FullPlane),
        line: Box::new(ContinuumMask::Boxes(vec![ContinuumMaskBox {
            blc: [7, 7],
            trc: [8, 8],
        }])),
    };

    let error = match execute_continuum(imaging) {
        Ok(_) => panic!("optional joint reconstruction reached production execution"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("JointContinuumLineReconstruction"),
        "wrong fail-closed error: {error}"
    );
    assert!(!PathBuf::from(format!("{}.psf", image_name.display())).exists());
}

#[test]
fn application_preserves_the_bounded_source_budget_across_multiple_rows() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = multi_row_measurement_set(root.path());
    let image_name = root.path().join("multi-row-dirty");

    let result = execute_continuum(request(
        measurement_set,
        image_name,
        ContinuumAlgorithm::Dirty,
    ))
    .expect("native multi-row dirty execution");
    let receipt = result.outcome.output.initial_receipt;
    let source_read = receipt
        .plan_node_identities()
        .into_iter()
        .find(|node| node.as_str().starts_with("transaction-read-initial-major"))
        .expect("initial source-read node");
    let source_buffer = casa_imaging_runtime::LeaseResource::IoBuffer(
        casa_imaging_runtime::IoBufferKind::SourceReadAhead,
    );
    let io_lifetime =
        casa_imaging_runtime::ClaimLifetime::through_fence(casa_imaging_runtime::FenceKind::Io);
    assert_eq!(
        receipt.planned_resource_amount(&source_read, &source_buffer, &io_lifetime),
        Some(64 << 20),
        "the application must preserve the admitted caller-owned source budget"
    );
    let (_, operations) = receipt
        .stage_actual_io(
            &source_read,
            casa_imaging_runtime::IoBufferKind::SourceReadAhead,
        )
        .expect("measured selected-observation source reads");

    assert_eq!(
        operations, 19,
        "the caller's admitted content budget should fill all eight rows in one bounded block"
    );
}

#[test]
fn application_compiles_common_beam_requests_with_common_spectral_coupling() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("common-beam");
    let mut imaging = request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Dirty,
    );
    imaging.beam_policy = ContinuumBeamPolicy::Common;

    let result = execute_continuum(imaging).expect("native common-beam application execution");
    assert_dirty_products(&image_name, &result.product_names);
    let restored =
        PagedImage::<f32>::open(PathBuf::from(format!("{}.image", image_name.display())))
            .expect("reopen common-beam restored image");
    assert!(
        restored
            .image_info()
            .expect("read restored image info")
            .beam_set
            .has_single_beam()
    );
}

#[test]
fn mtmfs_via_cube_executes_one_bounded_sixteen_channel_axis_from_four_spectral_windows() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = four_spw_aca_measurement_set(root.path());
    let image_name = root.path().join("four-spw-mvc");
    let mut imaging = request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Mtmfs {
            terms: 2,
            scales_px: vec![0.0],
            small_scale_bias: 0.0,
        },
    );
    imaging.data_description = None;
    imaging.channel_count = Some(8);
    imaging.maximum_major_cycles = Some(1);
    imaging.task_requirements = vec![
        TaskRequirement::SpectralMtmfsViaCube,
        TaskRequirement::SerialCpu,
    ];
    imaging.spectral_mode = SpectralImagingMode::MtmfsViaCube {
        axis: CubeAxisConfig {
            outframe: FrequencyRef::TOPO,
            ..CubeAxisConfig::default()
        },
        output_channels: Some(16),
    };

    let result = execute_continuum(imaging).expect("bounded multi-SPW MVC execution");
    assert_eq!(
        result.product_names,
        [
            ".psf.tt0",
            ".psf.tt1",
            ".psf.tt2",
            ".residual.tt0",
            ".residual.tt1",
            ".model.tt0",
            ".model.tt1",
            ".image.tt0",
            ".image.tt1",
            ".sumwt.tt0",
            ".sumwt.tt1",
            ".sumwt.tt2",
            ".mask",
            ".alpha",
            ".alpha.error",
        ]
        .map(str::to_string),
    );
    assert!(
        result
            .outcome
            .output
            .scientific
            .normal_state()
            .sample_count()
            > 0
    );
    for suffix in &result.product_names {
        let product =
            PagedImage::<f32>::open(PathBuf::from(format!("{}{suffix}", image_name.display())))
                .expect("reopen MVC Taylor product");
        assert_eq!(
            product.shape(),
            if suffix.starts_with(".sumwt.") {
                &[1, 1, 1, 1]
            } else {
                &[16, 16, 1, 1]
            },
            "{suffix}"
        );
    }
}

#[test]
fn cube_common_beam_products_preserve_blank_validity_beams_units_and_descending_wcs() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = spectral_line_measurement_set(root.path());
    let image_name = root.path().join("common-beam-cube");
    let mut imaging = request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Dirty,
    );
    imaging.spectral_window = Some("0:0~3".to_string());
    imaging.channel_count = Some(4);
    imaging.spectral_mode = SpectralImagingMode::Cube {
        axis: CubeAxisConfig {
            outframe: FrequencyRef::TOPO,
            start: Some(CubeAxisValue::Channel(3)),
            width: Some(CubeAxisValue::Channel(-1)),
            ..CubeAxisConfig::default()
        },
        output_channels: Some(4),
    };
    imaging.beam_policy = ContinuumBeamPolicy::Common;

    let result = execute_continuum(imaging).expect("native common-beam cube execution");
    assert_dirty_products(&image_name, &result.product_names);
    let open = |suffix: &str| {
        PagedImage::<f32>::open(PathBuf::from(format!("{}{suffix}", image_name.display())))
            .expect("reopen cube product")
    };
    let psf = open(".psf");
    let residual = open(".residual");
    let restored = open(".image");
    for product in [&psf, &residual, &restored] {
        assert_eq!(product.shape(), &[16, 16, 1, 4]);
    }
    assert_eq!(psf.units(), "Jy/beam");
    assert_eq!(residual.units(), "Jy/beam");
    assert_eq!(restored.units(), "Jy/beam");

    let psf_beams = psf.image_info().expect("PSF ImageInfo").beam_set;
    let residual_beams = residual.image_info().expect("residual ImageInfo").beam_set;
    assert!(psf_beams.equivalent(&residual_beams));
    assert!(
        restored
            .image_info()
            .expect("restored ImageInfo")
            .beam_set
            .has_single_beam()
    );
    let largest_valid = (1..4)
        .map(|channel| *psf_beams.beam(channel, 0))
        .max_by(|left, right| left.area().total_cmp(&right.area()))
        .expect("valid fitted beams");
    assert_eq!(*psf_beams.beam(0, 0), largest_valid);

    for product in [&residual, &restored] {
        assert_eq!(product.default_mask_name().as_deref(), Some("mask0"));
        let blank = product
            .get_mask_slice(&[0, 0, 0, 0], &[16, 16, 1, 1], &[1; 4])
            .expect("blank-channel mask")
            .expect("product validity mask");
        assert!(blank.iter().all(|valid| !*valid));
        let valid = product
            .get_mask_slice(&[0, 0, 0, 1], &[16, 16, 1, 1], &[1; 4])
            .expect("valid-channel mask")
            .expect("product validity mask");
        assert!(valid.iter().all(|valid| *valid));
    }
    let first = restored
        .coordinates()
        .to_world(&[8.0, 8.0, 0.0, 0.0])
        .expect("first channel world coordinate");
    let second = restored
        .coordinates()
        .to_world(&[8.0, 8.0, 0.0, 1.0])
        .expect("second channel world coordinate");
    assert!(first[3] > second[3], "descending spectral WCS");
}

#[test]
fn t607_application_preserves_channel_topology_and_wcs_through_cube_planning() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = thirty_two_channel_measurement_set(root.path());
    let image_name = root.path().join("t607-channel-local-cube");
    let mut imaging = request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Dirty,
    );
    imaging.spectral_window = Some("0:0~31".to_string());
    imaging.channel_count = Some(32);
    imaging.spectral_mode = SpectralImagingMode::Cube {
        axis: CubeAxisConfig {
            outframe: FrequencyRef::TOPO,
            ..CubeAxisConfig::default()
        },
        output_channels: Some(32),
    };

    let result = execute_continuum(imaging).expect("native 32-channel cube execution");
    assert_dirty_products(&image_name, &result.product_names);
    assert_eq!(
        result.outcome.output.scientific.normal_state().catalog(),
        casa_imaging_reconstruction::NormalStateCatalog::UnnormalizedChannelSlabV1
    );
    let slab_depth = result
        .outcome
        .output
        .initial_receipt
        .initial_execution_knobs()
        .slab_depth;
    assert!((1..=32).contains(&slab_depth));
    let residual =
        PagedImage::<f32>::open(PathBuf::from(format!("{}.residual", image_name.display())))
            .expect("reopen 32-channel residual");
    assert_eq!(residual.shape(), &[16, 16, 1, 32]);
    let first = residual
        .coordinates()
        .to_world(&[8.0, 8.0, 0.0, 0.0])
        .expect("first channel world coordinate");
    let last = residual
        .coordinates()
        .to_world(&[8.0, 8.0, 0.0, 31.0])
        .expect("last channel world coordinate");
    assert!(last[3] > first[3], "ascending spectral WCS");
}

#[test]
fn application_executes_single_ddid_stokes_i_mfs_hogbom_with_one_iteration() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("hogbom");
    let mut imaging = request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Hogbom,
    );
    imaging.task_requirements = vec![TaskRequirement::SerialCpu, TaskRequirement::FixedTileCpu];

    let result = execute_continuum(imaging).expect("native Högbom application execution");

    assert_eq!(result.minor_iterations, 1);
    assert_eq!(
        result.minor_stop_reason,
        Some(ContinuumStopReason::IterationBound)
    );
    assert_eq!(
        result
            .outcome
            .output
            .minor_cycles
            .last()
            .expect("minor diagnostic")
            .recorded_components
            .len(),
        1
    );
    let output = &result.outcome.output;
    assert!(
        output.visibility_products.is_none(),
        "a no-write clean must not manufacture per-visibility diagnostics"
    );
    assert!(
        output.visibility_write_receipt.is_none(),
        "a no-write clean must not execute a selected-output traversal"
    );
    let final_receipt = output
        .final_major_receipt
        .as_ref()
        .expect("clean retains its terminal artifact-science receipt");
    assert_eq!(
        final_receipt.projected_resource_policy(),
        casa_imaging_runtime::ResourcePolicy::Balanced
    );
    let final_nodes = final_receipt.plan_node_identities();
    assert!(final_nodes.iter().any(|node| {
        node.as_str()
            .starts_with("gridded-normal-replay-final-major")
    }));
    assert!(
        final_nodes
            .iter()
            .all(|node| !node.as_str().starts_with("transaction-read-final-major")),
        "terminal artifact science must be the last observation-facing work"
    );
    assert_eq!(
        final_receipt
            .selected_alternative_projection()
            .demand
            .io_buffers
            .bytes(casa_imaging_runtime::IoBufferKind::SourceReadAhead),
        0
    );
    let selected = final_receipt.selected_alternative_projection();
    let planned_workers = selected.demand.workers.hard();
    assert!((1..=4).contains(&planned_workers));
    assert!(
        selected
            .id
            .as_str()
            .ends_with(&format!("-workers-{planned_workers}")),
        "normal application composition must submit the scalable replay template to the planner",
    );
    if std::thread::available_parallelism().is_ok_and(|threads| threads.get() > 1) {
        assert!(
            planned_workers > 1,
            "normal production planning must not pin replay to the serial baseline on a parallel host",
        );
    }
    assert_standard_products(&image_name, &result.product_names);
}

#[test]
fn application_serial_cpu_requirement_caps_replay_to_one_worker() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("serial-hogbom");
    let mut imaging = request(measurement_set, image_name, ContinuumAlgorithm::Hogbom);
    imaging.task_requirements = vec![TaskRequirement::SerialCpu];
    imaging.resource_policy =
        casa_imaging_runtime::ResourcePolicy::Explicit(casa_imaging_runtime::ResourceOverride {
            workers: Some(1),
            ..casa_imaging_runtime::ResourceOverride::default()
        });

    let result = execute_continuum(imaging).expect("serial native Högbom execution");
    let final_receipt = result
        .outcome
        .output
        .final_major_receipt
        .as_ref()
        .expect("serial clean retains its terminal replay receipt");
    assert_eq!(
        final_receipt.projected_resource_policy(),
        casa_imaging_runtime::ResourcePolicy::Explicit(casa_imaging_runtime::ResourceOverride {
            workers: Some(1),
            ..casa_imaging_runtime::ResourceOverride::default()
        })
    );
    assert_eq!(
        final_receipt
            .selected_alternative_projection()
            .demand
            .workers
            .hard(),
        1
    );
}

#[test]
fn application_algorithms_do_not_invent_a_flux_staleness_bound() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("model-envelope");
    let imaging = request(measurement_set, image_name, ContinuumAlgorithm::Clark);

    let result = execute_continuum(imaging).expect("exact Clark execution");

    assert!(
        result.minor_iterations > 0,
        "active Clark execution must make scientific progress"
    );
    assert_ne!(
        result.minor_stop_reason,
        Some(ContinuumStopReason::StalenessBound)
    );
}

#[test]
fn application_reconciles_between_bounded_minor_cycles() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("bounded-cycles");
    let mut imaging = request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Hogbom,
    );
    imaging.iterations = 3;
    imaging.cycle_iterations = 1;
    imaging.maximum_major_cycles = Some(3);
    imaging.gain = 0.37;
    imaging.threshold_jy = 1.0e-12;
    imaging.noise_sigma = Some(1.0e-12);
    imaging.cycle_factor = 1.4;

    let result = execute_continuum(imaging).expect("bounded multi-cycle execution");

    assert_eq!(result.minor_iterations, 3);
    assert_eq!(result.actual_minor_iterations, 3);
    assert_eq!(result.outcome.output.total_minor_iterations, 3);
    assert_eq!(result.outcome.output.total_actual_minor_iterations, 3);
    assert_eq!(result.outcome.output.major_cycle_count, 4);
    assert_eq!(result.outcome.output.minor_cycles.len(), 3);
    assert_eq!(
        result
            .outcome
            .output
            .minor_cycles
            .iter()
            .map(|cycle| cycle.cycle)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(
        result
            .outcome
            .output
            .minor_cycles
            .iter()
            .all(|cycle| cycle.iterations == 1)
    );
    assert_eq!(
        result
            .outcome
            .output
            .minor_cycles
            .iter()
            .map(|cycle| (
                cycle.iterations_entering,
                cycle.iterations,
                cycle.total_iterations,
                cycle.associated_replay_ordinal,
            ))
            .collect::<Vec<_>>(),
        vec![(0, 1, 1, 1), (1, 1, 2, 2), (2, 1, 3, 3)]
    );
    assert!(result.outcome.output.minor_cycles.iter().all(|cycle| {
        cycle.initial_peak_flux.is_finite()
            && cycle.final_peak_flux.is_finite()
            && cycle.global_threshold.is_finite()
            && cycle.effective_threshold.is_finite()
    }));
    assert_eq!(
        result.minor_stop_reason,
        Some(ContinuumStopReason::IterationBound)
    );
    assert_standard_products(&image_name, &result.product_names);
}

#[test]
fn application_uses_reported_iterations_for_casa_inclusive_continuation() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let mut imaging = request(
        measurement_set,
        root.path().join("unlimited-major-cycles"),
        ContinuumAlgorithm::Hogbom,
    );
    imaging.iterations = 3;
    imaging.cycle_iterations = 1;
    imaging.hogbom_iteration_accounting =
        casa_imaging_application::HogbomIterationAccounting::CasaInclusive;
    imaging.maximum_major_cycles = None;
    imaging.gain = 0.37;
    imaging.threshold_jy = 1.0e-12;
    imaging.noise_sigma = Some(1.0e-12);
    imaging.cycle_factor = 0.01;
    imaging.minimum_psf_fraction = 0.0;
    imaging.maximum_psf_fraction = 0.01;

    let result = execute_continuum(imaging).expect("unlimited major-cycle execution");

    assert_eq!(result.outcome.output.total_minor_iterations, 3);
    assert_eq!(result.outcome.output.total_actual_minor_iterations, 6);
    assert_eq!(result.outcome.output.minor_cycles.len(), 3);
    assert_eq!(result.outcome.output.major_cycle_count, 4);
    assert!(
        result
            .outcome
            .output
            .minor_cycles
            .iter()
            .all(|cycle| cycle.iterations == 1 && cycle.actual_iterations == 2),
        "every bound-stopped cycle charges one reported iteration after applying two components"
    );
}

#[test]
fn application_commits_exact_final_prediction_to_model_data() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("savemodel");
    let mut imaging = request(
        measurement_set.clone(),
        image_name,
        ContinuumAlgorithm::Hogbom,
    );
    imaging.save_model_column = true;
    imaging.task_requirements = vec![TaskRequirement::SerialCpu];
    imaging.resource_policy = casa_imaging_runtime::ResourcePolicy::Balanced;

    let result = execute_continuum(imaging).expect("native save-model application execution");
    let visibility = result
        .outcome
        .output
        .visibility_products
        .expect("final visibility completion");
    assert_eq!(visibility.sample_count(), 1);
    let model_receipt = result
        .outcome
        .output
        .visibility_write_receipt
        .as_ref()
        .expect("MODEL_DATA write is receipted by the selected-output traversal");
    assert_eq!(
        model_receipt.observation_transaction_publication_scope(),
        casa_imaging_runtime::ObservationTransactionPublicationScope::ReconstructionOnly
    );
    assert_eq!(model_receipt.publication_layout_count(), 0);
    let final_receipt = result
        .outcome
        .output
        .final_major_receipt
        .as_ref()
        .expect("save-model execution has a terminal science receipt");
    assert_ne!(model_receipt, final_receipt);
    let plan_nodes = final_receipt.plan_node_identities();
    let preparation = plan_nodes
        .iter()
        .find(|node| {
            node.as_str()
                .starts_with("final-model-preparation-final-major")
        })
        .expect("final-model preparation is planned");
    assert!(plan_nodes.iter().any(|node| {
        node.as_str()
            .starts_with("post-replay-reconciliation-final-major")
    }));
    assert!(plan_nodes.iter().any(|node| {
        node.as_str()
            .starts_with("gridded-normal-replay-final-major")
    }));
    assert!(
        plan_nodes
            .iter()
            .all(|node| !node.as_str().starts_with("transaction-read-final-major")),
        "terminal science never reopens the selected observation"
    );
    let science_demand = final_receipt.selected_alternative_projection().demand;
    assert_eq!(science_demand.locks.hard(), 0);
    assert_eq!(
        science_demand.file_descriptors.hard(),
        1,
        "the later-major plan owns exactly its private replay artifact handle"
    );
    assert_eq!(
        science_demand
            .io_buffers
            .bytes(casa_imaging_runtime::IoBufferKind::SourceReadAhead),
        0
    );
    assert!(
        plan_nodes
            .iter()
            .all(|node| !node.as_str().contains("stage-model")),
        "MODEL_DATA has no physical staging node"
    );
    let output_nodes = model_receipt.plan_node_identities();
    let terminal_pass = output_nodes
        .iter()
        .find(|node| node.as_str().starts_with("transaction-read-final-major"))
        .expect("the bounded selected-output pass is planned");
    assert_eq!(
        output_nodes
            .iter()
            .filter(|node| node.as_str().starts_with("transaction-read-final-major"))
            .count(),
        1,
        "MODEL_DATA uses exactly one selected-output traversal"
    );
    assert!(output_nodes.iter().all(|node| {
        !node
            .as_str()
            .starts_with("gridded-normal-replay-final-major")
    }));
    assert_ne!(preparation, terminal_pass);
    assert_eq!(
        model_receipt
            .stage_predicted_io(terminal_pass, casa_imaging_runtime::IoBufferKind::Writeback,),
        Some((16, 1)),
        "first creation writes the zero-initialized column and selected prediction"
    );
    assert_eq!(
        model_receipt
            .stage_actual_io(terminal_pass, casa_imaging_runtime::IoBufferKind::Writeback,),
        None,
        "the table adapter exposes no trustworthy physical byte counter"
    );
    let write_lifetime =
        casa_imaging_runtime::ClaimLifetime::through_fence(casa_imaging_runtime::FenceKind::Io);
    let write_stack = casa_imaging_runtime::LeaseResource::RuntimeOverhead(
        casa_imaging_runtime::RuntimeOverheadKind::ThreadStack,
    );
    assert_eq!(
        model_receipt.planned_resource_amount(terminal_pass, &write_stack, &write_lifetime),
        Some(2 * 1024 * 1024)
    );
    assert_eq!(
        model_receipt.actual_resource_peak(terminal_pass, &write_stack, &write_lifetime),
        Some(2 * 1024 * 1024)
    );
    let model_storage = casa_imaging_runtime::LeaseResource::Storage {
        demand_id: "serial-visibility-write-column".to_string(),
        use_kind: casa_imaging_runtime::StorageUseKind::FinalOutput,
    };
    assert_eq!(
        model_receipt.planned_resource_amount(terminal_pass, &model_storage, &write_lifetime),
        Some(8),
        "column creation reserves its new persistent capacity"
    );

    let reopened = MeasurementSet::open(&measurement_set).expect("reopen saved MODEL_DATA");
    let schema = reopened.main_table().schema().expect("MAIN schema");
    assert!(schema.contains_column("MODEL_DATA"));
    let model_column = reopened
        .data_column(VisibilityDataColumn::ModelData)
        .expect("MODEL_DATA was committed");
    let ArrayValue::Complex32(model) = model_column.get(0).expect("MODEL_DATA row") else {
        panic!("MODEL_DATA row is complex")
    };
    assert!(model[[0, 0]].re.is_finite());
    assert!(model[[0, 0]].im.is_finite());
    assert_ne!(model[[0, 0]], Complex32::new(0.0, 0.0));
    drop(reopened);

    let mut overwrite = request(
        measurement_set,
        root.path().join("savemodel-overwrite"),
        ContinuumAlgorithm::Hogbom,
    );
    overwrite.save_model_column = true;
    let overwrite_result =
        execute_continuum(overwrite).expect("native in-place MODEL_DATA overwrite");
    let overwrite_receipt = overwrite_result
        .outcome
        .output
        .visibility_write_receipt
        .as_ref()
        .expect("overwrite receipt");
    let overwrite_terminal_pass = overwrite_receipt
        .plan_node_identities()
        .iter()
        .find(|node| node.as_str().starts_with("transaction-read-final-major"))
        .expect("overwrite selected-output traversal")
        .clone();
    assert_eq!(
        overwrite_receipt.stage_predicted_io(
            &overwrite_terminal_pass,
            casa_imaging_runtime::IoBufferKind::Writeback,
        ),
        Some((8, 1)),
        "overwrite predicts only the selected in-place cell write"
    );
    assert_eq!(
        overwrite_receipt.planned_resource_amount(
            &overwrite_terminal_pass,
            &model_storage,
            &write_lifetime,
        ),
        None,
        "existing MODEL_DATA reserves no new full-column persistent capacity"
    );
}

#[test]
fn continuum_fit_only_channels_are_read_but_not_persisted_as_line_model_data() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = spectral_line_measurement_set(root.path());
    let mut imaging = request(
        measurement_set.clone(),
        root.path().join("continuum-subtracted-line"),
        ContinuumAlgorithm::Hogbom,
    );
    imaging.channel_start = Some(1);
    imaging.channel_count = Some(1);
    imaging.spectral_window = Some("0:1".to_string());
    let axis = CubeAxisConfig {
        outframe: FrequencyRef::TOPO,
        start: Some(CubeAxisValue::Channel(1)),
        width: Some(CubeAxisValue::Channel(1)),
        ..CubeAxisConfig::default()
    };
    imaging.spectral_mode = SpectralImagingMode::Cube {
        axis,
        output_channels: Some(1),
    };
    imaging.continuum_subtraction = Some(VisibilityContinuumSubtraction {
        fit_spw: "0:0;3".to_string(),
        fit_order: 0,
    });
    imaging.save_model_column = true;

    let result = execute_continuum(imaging).expect("line-only MODEL_DATA write");
    assert_eq!(
        result
            .outcome
            .output
            .visibility_products
            .expect("final visibility completion")
            .sample_count(),
        4,
        "only the output channel contributes final line predictions"
    );
    let reopened = MeasurementSet::open(&measurement_set).expect("reopen MODEL_DATA");
    let model_column = reopened
        .data_column(VisibilityDataColumn::ModelData)
        .expect("MODEL_DATA");
    let model = model_column.get(0).expect("MODEL_DATA row");
    let ArrayValue::Complex32(model) = model else {
        panic!("MODEL_DATA row is complex")
    };
    for correlation in 0..4 {
        assert_eq!(
            model[[correlation, 0]],
            Complex32::new(9.0, 9.0),
            "fit-only channel must remain untouched"
        );
        assert_ne!(
            model[[correlation, 1]],
            Complex32::new(9.0, 9.0),
            "output channel must receive the final line model"
        );
        assert_eq!(
            model[[correlation, 3]],
            Complex32::new(9.0, 9.0),
            "second fit-only channel must remain untouched"
        );
    }
}

#[test]
fn continuum_residual_persistence_overwrites_only_output_roles_in_the_terminal_pass() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = spectral_line_measurement_set(root.path());
    let before = MeasurementSet::open(&measurement_set).expect("open before persistence");
    let flags_before = before
        .main_table()
        .column_accessor("FLAG")
        .expect("FLAG")
        .get(0)
        .expect("read FLAG")
        .cloned();
    let weights_before = before
        .main_table()
        .column_accessor("WEIGHT")
        .expect("WEIGHT")
        .get(0)
        .expect("read WEIGHT")
        .cloned();
    drop(before);

    let mut imaging = request(
        measurement_set.clone(),
        root.path().join("persisted-continuum-residual"),
        ContinuumAlgorithm::Hogbom,
    );
    imaging.channel_start = Some(1);
    imaging.channel_count = Some(1);
    imaging.spectral_window = Some("0:1".to_string());
    imaging.spectral_mode = SpectralImagingMode::Cube {
        axis: CubeAxisConfig {
            outframe: FrequencyRef::TOPO,
            start: Some(CubeAxisValue::Channel(1)),
            width: Some(CubeAxisValue::Channel(1)),
            ..CubeAxisConfig::default()
        },
        output_channels: Some(1),
    };
    imaging.continuum_subtraction = Some(VisibilityContinuumSubtraction {
        fit_spw: "0:0;3".to_string(),
        fit_order: 0,
    });
    imaging.save_model_column = true;
    imaging.save_continuum_residual = true;

    let result = execute_continuum(imaging).expect("persist continuum residual");
    assert_eq!(result.outcome.output.major_cycle_count, 2);
    let receipt = result
        .outcome
        .output
        .visibility_write_receipt
        .expect("combined visibility-write receipt");
    assert_eq!(
        receipt
            .plan_node_identities()
            .into_iter()
            .filter(|node| node.as_str().starts_with("transaction-read-final-major"))
            .count(),
        1,
        "MODEL_DATA and CORRECTED_DATA share the one terminal replay"
    );

    let reopened = MeasurementSet::open(&measurement_set).expect("reopen persisted residual");
    let corrected_column = reopened
        .data_column(VisibilityDataColumn::CorrectedData)
        .expect("CORRECTED_DATA");
    let ArrayValue::Complex32(corrected) = corrected_column.get(0).expect("CORRECTED_DATA row")
    else {
        panic!("CORRECTED_DATA is complex")
    };
    for correlation in 0..4 {
        assert_eq!(
            corrected[[correlation, 0]],
            Complex32::new(20.0 + (correlation * 4) as f32, -3.0),
            "fit-only cells remain unchanged"
        );
        assert_eq!(
            corrected[[correlation, 1]],
            Complex32::new(1.0, 0.0),
            "output-role cells receive exact transformed observations"
        );
        assert_eq!(
            corrected[[correlation, 2]],
            Complex32::new(22.0 + (correlation * 4) as f32, -3.0),
            "nonselected cells remain unchanged"
        );
        assert_eq!(
            corrected[[correlation, 3]],
            Complex32::new(23.0 + (correlation * 4) as f32, -3.0),
            "second fit-only cells remain unchanged"
        );
    }
    assert_eq!(
        reopened
            .main_table()
            .column_accessor("FLAG")
            .expect("FLAG")
            .get(0)
            .expect("read FLAG")
            .cloned(),
        flags_before
    );
    assert_eq!(
        reopened
            .main_table()
            .column_accessor("WEIGHT")
            .expect("WEIGHT")
            .get(0)
            .expect("read WEIGHT")
            .cloned(),
        weights_before
    );
    assert!(!measurement_set.join(".casa-rs-write-incomplete").exists());
}

#[test]
fn dirty_continuum_residual_persistence_is_independent_of_model_writeback() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = spectral_line_measurement_set(root.path());
    let mut imaging = request(
        measurement_set.clone(),
        root.path().join("dirty-persisted-continuum-residual"),
        ContinuumAlgorithm::Dirty,
    );
    imaging.channel_start = Some(1);
    imaging.channel_count = Some(1);
    imaging.spectral_window = Some("0:1".to_string());
    imaging.spectral_mode = SpectralImagingMode::Cube {
        axis: CubeAxisConfig {
            outframe: FrequencyRef::TOPO,
            start: Some(CubeAxisValue::Channel(1)),
            width: Some(CubeAxisValue::Channel(1)),
            ..CubeAxisConfig::default()
        },
        output_channels: Some(1),
    };
    imaging.continuum_subtraction = Some(VisibilityContinuumSubtraction {
        fit_spw: "0:0;3".to_string(),
        fit_order: 0,
    });
    imaging.save_continuum_residual = true;
    assert!(!imaging.save_model_column);

    let result = execute_continuum(imaging).expect("dirty residual-only persistence");
    assert_eq!(result.outcome.output.major_cycle_count, 1);
    let receipt = result
        .outcome
        .output
        .visibility_write_receipt
        .expect("initial terminal visibility-write receipt");
    assert_eq!(
        receipt
            .plan_node_identities()
            .into_iter()
            .filter(|node| node.as_str().starts_with("transaction-read-initial-major"))
            .count(),
        1,
        "dirty persistence reuses its sole observation pass"
    );

    let reopened = MeasurementSet::open(&measurement_set).expect("reopen residual-only MS");
    let corrected_column = reopened
        .data_column(VisibilityDataColumn::CorrectedData)
        .expect("CORRECTED_DATA");
    let ArrayValue::Complex32(corrected) = corrected_column.get(0).expect("CORRECTED_DATA row")
    else {
        panic!("CORRECTED_DATA is complex")
    };
    for correlation in 0..4 {
        assert_eq!(corrected[[correlation, 1]], Complex32::new(1.0, 0.0));
    }
    let model_column = reopened
        .data_column(VisibilityDataColumn::ModelData)
        .expect("MODEL_DATA");
    let ArrayValue::Complex32(model) = model_column.get(0).expect("MODEL_DATA row") else {
        panic!("MODEL_DATA is complex")
    };
    assert!(
        model.iter().all(|value| *value == Complex32::new(9.0, 9.0)),
        "residual-only persistence leaves MODEL_DATA untouched"
    );
}

#[test]
fn application_replaces_every_selected_model_cell_when_flags_and_correlations_differ() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = flagged_polarized_measurement_set(root.path());
    let mut imaging = request(
        measurement_set.clone(),
        root.path().join("polarized-savemodel"),
        ContinuumAlgorithm::Hogbom,
    );
    imaging.channel_count = Some(2);
    imaging.save_model_column = true;

    let result = execute_continuum(imaging).expect("partially flagged MODEL_DATA write");
    let visibility = result
        .outcome
        .output
        .visibility_products
        .expect("terminal visibility completion");
    assert_eq!(
        visibility.sample_count(),
        8,
        "the sink covers all selected rows, channels, and correlations"
    );
    let receipt = result
        .outcome
        .output
        .visibility_write_receipt
        .expect("MODEL_DATA receipt");
    let terminal_pass = receipt
        .plan_node_identities()
        .into_iter()
        .find(|node| node.as_str().starts_with("transaction-read-final-major"))
        .expect("single selected-output traversal");
    assert_eq!(
        receipt.stage_predicted_io(
            &terminal_pass,
            casa_imaging_runtime::IoBufferKind::Writeback,
        ),
        Some((64, 1)),
        "the existing column receives all eight selected Complex cells"
    );

    let reopened = MeasurementSet::open(&measurement_set).expect("reopen MODEL_DATA");
    let model_column = reopened
        .data_column(VisibilityDataColumn::ModelData)
        .expect("MODEL_DATA column");
    let ArrayValue::Complex32(model) = model_column.get(0).expect("MODEL_DATA row") else {
        panic!("MODEL_DATA row is complex")
    };
    let flag_column = reopened
        .main_table()
        .column_accessor("FLAG")
        .expect("FLAG column");
    let Value::Array(ArrayValue::Bool(flags)) = flag_column
        .get(0)
        .expect("read FLAG row")
        .cloned()
        .expect("defined FLAG row")
    else {
        panic!("FLAG row is boolean")
    };
    assert!(
        model.iter().all(|value| *value != Complex32::new(9.0, 9.0)),
        "no selected destination retains its stale pre-run value"
    );
    for correlation in 0..4 {
        for channel in 0..2 {
            let model_value = model[[correlation, channel]];
            let parallel_hand = matches!(correlation, 0 | 3);
            if flags[[correlation, channel]] || !parallel_hand {
                assert_eq!(
                    model_value,
                    Complex32::new(0.0, 0.0),
                    "flagged and unsupported cross-hand predictions persist as CASA zeros"
                );
            } else {
                assert_ne!(
                    model_value,
                    Complex32::new(0.0, 0.0),
                    "unflagged parallel-hand predictions retain the solved Stokes-I model"
                );
            }
        }
    }
}

#[test]
fn application_materializes_static_and_auto_masks_at_the_normal_state_boundary() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");

    let static_ms = tiny_measurement_set(root.path());
    let static_image = root.path().join("static-mask");
    let mut static_request = request(static_ms, static_image.clone(), ContinuumAlgorithm::Hogbom);
    static_request.mask = ContinuumMask::Boxes(vec![ContinuumMaskBox {
        blc: [4, 4],
        trc: [11, 11],
    }]);
    let static_result = execute_continuum(static_request).expect("static-mask solve");
    assert!(
        static_result
            .outcome
            .output
            .minor_cycles
            .last()
            .expect("minor-cycle evidence")
            .auto_mask
            .is_none()
    );
    let published_mask = PagedImage::<f32>::open(root.path().join("static-mask.mask"))
        .expect("open published reconstruction mask");
    let mask_pixels = published_mask
        .get_slice(&[0, 0, 0, 0], &[16, 16, 1, 1])
        .expect("read published reconstruction mask");
    assert_eq!(mask_pixels[[0, 0, 0, 0]], 0.0);
    assert_eq!(mask_pixels[[8, 8, 0, 0]], 1.0);
    assert_model_residual_respect_mask(&static_image, 64);

    let image_root = root.path().join("image-mask-input");
    std::fs::create_dir(&image_root).expect("image-mask fixture directory");
    let image_ms = tiny_measurement_set(&image_root);
    let mask_path = root.path().join("shifted.mask");
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        casa_types::measures::direction::DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [1.0, 0.5],
        [
            -std::f64::consts::PI / (180.0 * 3600.0),
            std::f64::consts::PI / (180.0 * 3600.0),
        ],
        [9.0, 8.0],
    ));
    let mut image = PagedImage::<f32>::create(vec![16, 16], coordinates, &mask_path)
        .expect("create shifted CASA image mask");
    let mut pixels = ArrayD::from_elem(ndarray::IxDyn(&[16, 16]), 0.0_f32);
    pixels[[3, 3]] = 1.0;
    image
        .put_slice(&pixels, &[0, 0])
        .expect("write mask pixels");
    image.save().expect("persist image mask");
    let image_output = root.path().join("image-mask");
    let mut image_request = request(image_ms, image_output.clone(), ContinuumAlgorithm::Hogbom);
    image_request.mask = ContinuumMask::Image(mask_path);
    let image_result = execute_continuum(image_request).expect("reprojected image-mask solve");
    assert!(!image_result.outcome.output.minor_cycles.is_empty());
    let reprojected_mask = product_plane(&image_output, ".mask");
    assert_eq!(reprojected_mask[[2, 3, 0, 0]], 1.0);
    assert_model_residual_respect_mask(&image_output, 1);

    let auto_root = root.path().join("auto-input");
    std::fs::create_dir(&auto_root).expect("auto fixture directory");
    let auto_ms = tiny_measurement_set(&auto_root);
    let mut auto_request = request(
        auto_ms,
        root.path().join("auto-mask"),
        ContinuumAlgorithm::Hogbom,
    );
    auto_request.mask = ContinuumMask::AutoMultithresh(ContinuumAutoMaskControls {
        sidelobe_factor: 0.0,
        noise_factor: 0.0,
        low_noise_factor: 0.0,
        negative_factor: 0.0,
        minimum_beam_fraction: 0.0,
        smooth_factor: 1.0,
        cut_threshold: 0.01,
        grow_iterations: 0,
        minimum_percent_change: -1.0,
    });
    auto_request.iterations = 2;
    auto_request.cycle_iterations = 1;
    auto_request.maximum_major_cycles = Some(2);
    auto_request.gain = 0.1;
    let auto_result = execute_continuum(auto_request).expect("auto-mask solve");
    let cycles = &auto_result.outcome.output.minor_cycles;
    assert_eq!(cycles.len(), 2);
    let first_evidence = cycles[0].auto_mask.expect("first auto-mask evidence");
    assert_eq!(first_evidence.previous_mask_generation, None);
    let evidence = cycles[1].auto_mask.expect("second auto-mask evidence");
    assert_eq!(
        evidence.previous_mask_generation,
        Some(cycles[0].mask_generation),
        "the next automatic mask must retain the exact prior generation"
    );
    assert!(cycles.iter().all(|cycle| cycle.mask_normal_state.is_some()));
    assert_ne!(
        cycles[0].mask_normal_state, cycles[1].mask_normal_state,
        "each automatic-mask generation must consume the current reconciled Normal State"
    );
    assert_ne!(
        cycles[0].mask_model_generation, cycles[1].mask_model_generation,
        "each automatic mask must constrain the current model generation"
    );
    assert!(evidence.robust_rms.is_finite());
    assert!(evidence.positive_threshold.is_finite());
    assert_eq!(auto_result.outcome.output.major_cycle_count, 3);
    assert_eq!(auto_result.minor_iterations, 2);
}
