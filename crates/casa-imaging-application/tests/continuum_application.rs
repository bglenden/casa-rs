// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

use casa_coordinates::{CoordinateSystem, DirectionCoordinate, Projection, ProjectionType};
use casa_images::PagedImage;
use casa_imaging_application::{
    ContinuumAlgorithm, ContinuumAutoMaskControls, ContinuumBeamPolicy, ContinuumImagingRequest,
    ContinuumMask, ContinuumMaskBox, ContinuumStopReason, ContinuumWeighting, execute_continuum,
};
use casa_ms::{
    MeasurementSet, MeasurementSetBuilder, OptionalMainColumn, SubtableId, VisibilityDataColumn,
    column_def::{ColumnDef, ColumnKind},
    initialize_measurement_set_owner_manifest, schema,
};
use casa_types::{
    ArrayValue, Complex32, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::ArrayD;

const PRODUCT_SUFFIXES: [&str; 5] = [".psf", ".residual", ".model", ".image", ".sumwt"];

static EXECUTION_LOCK: Mutex<()> = Mutex::new(());

fn tiny_measurement_set(root: &Path) -> PathBuf {
    let output = root.join("input.ms");
    let mut measurement_set = MeasurementSet::create_memory(
        MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
    )
    .expect("create in-memory application fixture");
    populate_fixture(&mut measurement_set);
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

fn populate_fixture(measurement_set: &mut MeasurementSet) {
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
                ("NUM_CORR", int(1)),
                (
                    "CORR_TYPE",
                    Value::Array(ArrayValue::Int32(
                        ArrayD::from_shape_vec(vec![1], vec![5]).expect("RR shape"),
                    )),
                ),
                (
                    "CORR_PRODUCT",
                    Value::Array(ArrayValue::Int32(
                        ArrayD::from_shape_vec(vec![2, 1], vec![0, 0])
                            .expect("receptor-pair shape"),
                    )),
                ),
                ("FLAG_ROW", boolean(false)),
            ],
        ))
        .expect("add POLARIZATION row");

    let frequency = Value::Array(ArrayValue::Float64(
        ArrayD::from_shape_vec(vec![1], vec![44.0e9]).expect("frequency shape"),
    ));
    let width = Value::Array(ArrayValue::Float64(
        ArrayD::from_shape_vec(vec![1], vec![1.0e6]).expect("width shape"),
    ));
    measurement_set
        .subtable_mut(SubtableId::SpectralWindow)
        .expect("SPECTRAL_WINDOW")
        .add_row(required_row(
            schema::spectral_window::REQUIRED_COLUMNS,
            &[
                ("NUM_CHAN", int(1)),
                ("NAME", string("CONTINUUM")),
                ("REF_FREQUENCY", float(44.0e9)),
                ("TOTAL_BANDWIDTH", float(1.0e6)),
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

    add_main_row(
        measurement_set,
        &[
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
                    ArrayD::from_shape_vec(vec![1, 1], vec![Complex32::new(1.0, 0.0)])
                        .expect("DATA shape"),
                )),
            ),
            (
                "FLAG",
                Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(vec![1, 1], vec![false]).expect("FLAG shape"),
                )),
            ),
            (
                "WEIGHT",
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(vec![1], vec![1.0]).expect("WEIGHT shape"),
                )),
            ),
            (
                "SIGMA",
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(vec![1], vec![1.0]).expect("SIGMA shape"),
                )),
            ),
            ("FLAG_ROW", boolean(false)),
        ],
    );
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
        data_column: Some("DATA".to_string()),
        algorithm,
        weighting: ContinuumWeighting::Natural,
        iterations: 1,
        cycle_iterations: 1,
        maximum_major_cycles: 1,
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
}

#[test]
fn application_executes_single_ddid_stokes_i_mfs_hogbom_with_one_iteration() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("hogbom");

    let result = execute_continuum(request(
        measurement_set,
        image_name.clone(),
        ContinuumAlgorithm::Hogbom,
    ))
    .expect("native Högbom application execution");

    assert_eq!(result.minor_iterations, 1);
    assert_eq!(
        result.minor_stop_reason,
        Some(ContinuumStopReason::IterationBound)
    );
    let visibility = result
        .outcome
        .output
        .visibility_products
        .expect("final major replay closes visibility products");
    assert_eq!(visibility.sample_count(), 1);
    assert_eq!(
        visibility.final_model(),
        result
            .outcome
            .output
            .scientific
            .final_model()
            .generation_id()
    );
    assert_ne!(
        visibility.model_product().as_bytes(),
        visibility.residual_product().as_bytes(),
        "model and residual visibility products have distinct meanings"
    );
    assert_standard_products(&image_name, &result.product_names);
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
    imaging.maximum_major_cycles = 3;
    imaging.gain = 0.1;

    let result = execute_continuum(imaging).expect("bounded multi-cycle execution");

    assert_eq!(result.minor_iterations, 3);
    assert_eq!(result.outcome.output.total_minor_iterations, 3);
    assert_eq!(result.outcome.output.major_cycle_count, 4);
    assert_eq!(
        result.minor_stop_reason,
        Some(ContinuumStopReason::IterationBound)
    );
    assert_standard_products(&image_name, &result.product_names);
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
        .model_data_receipt
        .as_ref()
        .expect("MODEL_DATA commit is independently receipted");
    assert_eq!(
        model_receipt.observation_transaction_publication_scope(),
        casa_imaging_runtime::ObservationTransactionPublicationScope::ModelDataPublication
    );
    assert_eq!(model_receipt.publication_layout_count(), 1);

    let reopened = MeasurementSet::open(measurement_set).expect("reopen saved MODEL_DATA");
    let model_column = reopened
        .data_column(VisibilityDataColumn::ModelData)
        .expect("MODEL_DATA was committed");
    let ArrayValue::Complex32(model) = model_column.get(0).expect("MODEL_DATA row") else {
        panic!("MODEL_DATA row is complex")
    };
    assert!(model[[0, 0]].re.is_finite());
    assert!(model[[0, 0]].im.is_finite());
    assert_ne!(model[[0, 0]], Complex32::new(0.0, 0.0));
}

#[test]
fn application_materializes_static_and_auto_masks_at_the_normal_state_boundary() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");

    let static_ms = tiny_measurement_set(root.path());
    let mut static_request = request(
        static_ms,
        root.path().join("static-mask"),
        ContinuumAlgorithm::Hogbom,
    );
    static_request.mask = ContinuumMask::Boxes(vec![ContinuumMaskBox {
        blc: [0, 0],
        trc: [15, 15],
    }]);
    let static_result = execute_continuum(static_request).expect("static-mask solve");
    assert!(
        static_result
            .outcome
            .output
            .minor_cycle
            .expect("minor-cycle evidence")
            .auto_mask
            .is_none()
    );

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
    let mut image_request = request(
        image_ms,
        root.path().join("image-mask"),
        ContinuumAlgorithm::Hogbom,
    );
    image_request.mask = ContinuumMask::Image(mask_path);
    let image_result = execute_continuum(image_request).expect("reprojected image-mask solve");
    assert!(image_result.outcome.output.minor_cycle.is_some());

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
    auto_request.maximum_major_cycles = 2;
    auto_request.gain = 0.1;
    let auto_result = execute_continuum(auto_request).expect("auto-mask solve");
    let evidence = auto_result
        .outcome
        .output
        .minor_cycle
        .expect("minor-cycle evidence")
        .auto_mask
        .expect("auto-mask evidence");
    assert!(evidence.robust_rms.is_finite());
    assert!(evidence.positive_threshold.is_finite());
    assert_eq!(auto_result.outcome.output.major_cycle_count, 3);
    assert_eq!(auto_result.minor_iterations, 2);
}
