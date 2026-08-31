// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

use casa_coordinates::{CoordinateSystem, DirectionCoordinate, Projection, ProjectionType};
use casa_images::PagedImage;
use casa_imaging_application::{
    ContinuumAlgorithm, ContinuumAutoMaskControls, ContinuumBeamPolicy, ContinuumImagingRequest,
    ContinuumMask, ContinuumMaskBox, ContinuumStopReason, ContinuumWeighting, SpectralImagingMode,
    TaskRequirement, VisibilityContinuumSubtraction, execute_continuum,
};
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

static EXECUTION_LOCK: Mutex<()> = Mutex::new(());

fn tiny_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(root, "input.ms", false, 1, 1)
}

fn multi_row_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(root, "multi-row-input.ms", false, 1, 8)
}

fn flagged_polarized_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(root, "polarized-input.ms", true, 2, 1)
}

fn spectral_line_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(root, "line-input.ms", true, 4, 1)
}

fn joint_measurement_set(root: &Path) -> PathBuf {
    measurement_set_fixture(root, "joint-input.ms", false, 4, 1)
}

fn measurement_set_fixture(
    root: &Path,
    name: &str,
    polarized: bool,
    channel_count: usize,
    main_row_count: usize,
) -> PathBuf {
    let output = root.join(name);
    let mut builder = MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data);
    if polarized {
        builder = builder.with_main_column(OptionalMainColumn::ModelData);
    }
    if channel_count == 4 {
        builder = builder.with_main_column(OptionalMainColumn::CorrectedData);
    }
    let mut measurement_set =
        MeasurementSet::create_memory(builder).expect("create in-memory application fixture");
    populate_fixture(
        &mut measurement_set,
        polarized,
        channel_count,
        main_row_count,
    );
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

fn populate_fixture(
    measurement_set: &mut MeasurementSet,
    polarized: bool,
    channel_count: usize,
    main_row_count: usize,
) {
    {
        let mut antennas = measurement_set.antenna_mut().expect("ANTENNA");
        antennas
            .add_antenna(
                "VLA01",
                "N01",
                "GROUND-BASED",
                "ALT-AZ",
                [-1_601_185.4, -5_041_977.5, 3_554_875.9],
                [0.0; 3],
                25.0,
            )
            .expect("add first antenna");
        antennas
            .add_antenna(
                "VLA02",
                "N02",
                "GROUND-BASED",
                "ALT-AZ",
                [-1_601_085.4, -5_041_977.5, 3_554_875.9],
                [0.0; 3],
                25.0,
            )
            .expect("add second antenna");
    }

    let direction = ArrayValue::Float64(
        ArrayD::from_shape_vec(vec![2, 1], vec![1.0, 0.5]).expect("direction shape"),
    );
    let correlation_codes = if polarized { vec![5, 6, 7, 8] } else { vec![5] };
    let correlation_count = correlation_codes.len();
    let correlation_products = if polarized {
        vec![0, 0, 0, 1, 1, 0, 1, 1]
    } else {
        vec![0, 0]
    };
    measurement_set
        .subtable_mut(SubtableId::Field)
        .expect("FIELD")
        .add_row(required_row(
            schema::field::REQUIRED_COLUMNS,
            &[
                ("NAME", string("APPLICATION_FIELD")),
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

    let frequency = Value::Array(ArrayValue::Float64(
        ArrayD::from_shape_vec(
            vec![channel_count],
            (0..channel_count)
                .map(|channel| 44.0e9 + channel as f64 * 1.0e6)
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
                ("NAME", string("CONTINUUM")),
                ("REF_FREQUENCY", float(44.0e9)),
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
                ("SPECTRAL_WINDOW_ID", int(0)),
                ("POLARIZATION_ID", int(0)),
                ("FLAG_ROW", boolean(false)),
            ],
        ))
        .expect("add DATA_DESCRIPTION row");

    let visibilities = (0..correlation_count * channel_count)
        .map(|index| Complex32::new((index % 6 + 1) as f32, 0.0))
        .collect::<Vec<_>>();
    let flags = (0..correlation_count * channel_count)
        .map(|index| polarized && index % 4 == 3)
        .collect::<Vec<_>>();
    let weights = vec![1.0; correlation_count];
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
    for _ in 0..main_row_count {
        add_main_row(measurement_set, &overrides);
    }
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
        cell_arcsec: 1.0,
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
        beam_policy: ContinuumBeamPolicy::PerPlane,
        mask: ContinuumMask::FullPlane,
        save_model_column: false,
        save_continuum_residual: false,
        write_primary_beam: false,
        pbcor: false,
        task_requirements: Vec::new(),
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
    let expected = PRODUCT_SUFFIXES
        .iter()
        .map(|suffix| (*suffix).to_string())
        .collect::<Vec<_>>();
    assert_eq!(product_names, expected);
    for suffix in PRODUCT_SUFFIXES {
        let path = PathBuf::from(format!("{}{}", image_name.display(), suffix));
        assert!(
            path.is_dir(),
            "missing CASA product directory {}",
            path.display()
        );
    }
}

fn product_plane(image_name: &Path, suffix: &str) -> ArrayD<f32> {
    PagedImage::<f32>::open(PathBuf::from(format!("{}{}", image_name.display(), suffix)))
        .expect("open application product")
        .get_slice(&[0, 0, 0, 0], &[16, 16, 1, 1])
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
    assert_standard_products(&image_name, &result.product_names);
    for suffix in [".residual", ".image"] {
        let product =
            PagedImage::<f32>::open(PathBuf::from(format!("{}{}", image_name.display(), suffix)))
                .expect("reopen validity-bearing product");
        assert_eq!(product.default_mask_name().as_deref(), Some("mask0"));
    }
    let clean_mask =
        PagedImage::<f32>::open(PathBuf::from(format!("{}.mask", image_name.display())))
            .expect("reopen numeric CLEAN mask");
    assert_eq!(clean_mask.default_mask_name(), None);
}

#[test]
fn t46_application_executes_joint_continuum_line_through_one_native_route() {
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

    let result = execute_continuum(imaging).expect("native joint application execution");
    for suffix in [
        ".total.residual",
        ".continuum.model.ct0",
        ".line.model",
        ".total.model",
        ".line.image",
        ".total.image",
        ".continuum.mask",
        ".line.mask",
    ] {
        assert!(
            result.product_names.iter().any(|name| name == suffix),
            "missing joint product {suffix}: {:?}",
            result.product_names
        );
        assert!(
            PathBuf::from(format!("{}{suffix}", image_name.display())).is_dir(),
            "missing persisted joint product {suffix}"
        );
    }
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
    assert_standard_products(&image_name, &result.product_names);
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
    assert_standard_products(&image_name, &result.product_names);
    let open = |suffix: &str| {
        PagedImage::<f32>::open(PathBuf::from(format!("{}{suffix}", image_name.display())))
            .expect("reopen cube product")
    };
    let psf = open(".psf");
    let residual = open(".residual");
    let restored = open(".image");
    let clean_mask = open(".mask");
    for product in [&psf, &residual, &restored, &clean_mask] {
        assert_eq!(product.shape(), &[16, 16, 1, 4]);
    }
    assert_eq!(psf.units(), "Jy/beam");
    assert_eq!(residual.units(), "Jy/beam");
    assert_eq!(restored.units(), "Jy/beam");
    assert_eq!(clean_mask.units(), "");

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
    assert_eq!(clean_mask.default_mask_name(), None);

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
    assert!(
        model.iter().all(|value| *value != Complex32::new(9.0, 9.0)),
        "no selected destination retains its stale pre-run value"
    );
    for channel in 0..2 {
        assert_ne!(model[[0, channel]], Complex32::new(0.0, 0.0));
        assert_ne!(model[[3, channel]], Complex32::new(0.0, 0.0));
        assert_eq!(model[[1, channel]], Complex32::new(0.0, 0.0));
        assert_eq!(model[[2, channel]], Complex32::new(0.0, 0.0));
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
