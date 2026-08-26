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

const PRODUCT_SUFFIXES: [&str; 6] = [".psf", ".residual", ".model", ".image", ".sumwt", ".mask"];

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
        maximum_model_update_jy: 100.0,
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
    let visibility = result
        .outcome
        .output
        .visibility_products
        .expect("final major replay closes visibility products");
    let normal = result.outcome.output.scientific.normal_state();
    assert_eq!(visibility.sample_count(), 1);
    assert_eq!(visibility.problem_id(), normal.problem_id());
    assert_eq!(
        visibility.selected_generation(),
        normal.selected_generation()
    );
    assert_eq!(
        visibility.weighting_generation(),
        normal.weighting_generation()
    );
    assert_eq!(visibility.sample_count(), normal.sample_count());
    assert_eq!(visibility.final_model(), normal.final_model_generation());
    assert_ne!(
        visibility.model_product().as_bytes(),
        visibility.residual_product().as_bytes(),
        "model and residual visibility products have distinct meanings"
    );
    assert_standard_products(&image_name, &result.product_names);
}

#[test]
fn application_enforces_requested_model_update_envelope_before_mutation() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    set_production_io_environment();
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = tiny_measurement_set(root.path());
    let image_name = root.path().join("model-envelope");
    let mut imaging = request(measurement_set, image_name, ContinuumAlgorithm::Clark);
    imaging.maximum_model_update_jy = f64::EPSILON;

    let result = execute_continuum(imaging).expect("bounded Clark execution");

    assert_eq!(result.minor_iterations, 0);
    assert_eq!(
        result.minor_stop_reason,
        Some(ContinuumStopReason::StalenessBound)
    );
    assert!(
        result
            .outcome
            .output
            .minor_cycles
            .last()
            .expect("minor diagnostic")
            .recorded_components
            .is_empty()
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
    imaging.maximum_major_cycles = 3;
    imaging.gain = 0.37;
    imaging.threshold_jy = 1.0e-12;
    imaging.noise_sigma = Some(1.0e-12);
    imaging.cycle_factor = 1.4;

    let result = execute_continuum(imaging).expect("bounded multi-cycle execution");

    assert_eq!(result.minor_iterations, 3);
    assert_eq!(result.outcome.output.total_minor_iterations, 3);
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
        .expect("MODEL_DATA write is receipted by the terminal replay");
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
        .expect("save-model execution has a terminal replay receipt");
    assert_eq!(model_receipt, final_receipt);
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
    assert!(
        plan_nodes
            .iter()
            .all(|node| !node.as_str().contains("stage-model")),
        "MODEL_DATA has no physical staging node"
    );
    let replay = plan_nodes
        .iter()
        .find(|node| node.as_str().starts_with("weighting-replay-final-major"))
        .expect("terminal replay is planned");
    assert_ne!(preparation, replay);
    assert_eq!(
        final_receipt.stage_predicted_io(replay, casa_imaging_runtime::IoBufferKind::Writeback),
        Some((16, 1)),
        "first creation writes the zero-initialized column and selected prediction"
    );
    assert_eq!(
        final_receipt.stage_actual_io(replay, casa_imaging_runtime::IoBufferKind::Writeback),
        None,
        "the table adapter exposes no trustworthy physical byte counter"
    );
    let write_lifetime =
        casa_imaging_runtime::ClaimLifetime::through_fence(casa_imaging_runtime::FenceKind::Io);
    let write_stack = casa_imaging_runtime::LeaseResource::RuntimeOverhead(
        casa_imaging_runtime::RuntimeOverheadKind::ThreadStack,
    );
    assert_eq!(
        final_receipt.planned_resource_amount(replay, &write_stack, &write_lifetime),
        Some(2 * 1024 * 1024)
    );
    assert_eq!(
        final_receipt.actual_resource_peak(replay, &write_stack, &write_lifetime),
        Some(2 * 1024 * 1024)
    );
    let model_storage = casa_imaging_runtime::LeaseResource::Storage {
        demand_id: "serial-model-data-column".to_string(),
        use_kind: casa_imaging_runtime::StorageUseKind::FinalOutput,
    };
    assert_eq!(
        final_receipt.planned_resource_amount(replay, &model_storage, &write_lifetime),
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
        .model_data_receipt
        .as_ref()
        .expect("overwrite receipt");
    let overwrite_replay = overwrite_receipt
        .plan_node_identities()
        .iter()
        .find(|node| node.as_str().starts_with("weighting-replay-final-major"))
        .expect("overwrite replay")
        .clone();
    assert_eq!(
        overwrite_receipt.stage_predicted_io(
            &overwrite_replay,
            casa_imaging_runtime::IoBufferKind::Writeback,
        ),
        Some((8, 1)),
        "overwrite predicts only the selected in-place cell write"
    );
    assert_eq!(
        overwrite_receipt.planned_resource_amount(
            &overwrite_replay,
            &model_storage,
            &write_lifetime,
        ),
        None,
        "existing MODEL_DATA reserves no new full-column persistent capacity"
    );
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
    auto_request.maximum_major_cycles = 2;
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
