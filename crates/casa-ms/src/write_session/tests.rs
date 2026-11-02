// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn plan_uses_shape_budget_queue_and_tile_alignment() {
    let plan = MeasurementSetWritePlan::visibility_creation(
        1_000,
        4,
        64,
        "VLA",
        MeasurementSetWriteResources {
            available_bytes: 64 * 1024 * 1024,
            maximum_live_batches: 3,
            tiled_column_buffer_bytes: 0,
        },
    )
    .expect("write plan");
    assert!(plan.batch_rows > 0);
    assert!(
        plan.batch_rows == plan.row_count
            || plan
                .batch_rows
                .is_multiple_of(plan.visibility_tile_shape[2])
    );
    assert!(plan.maximum_resident_bytes <= 64 * 1024 * 1024);
    assert!(plan.queue_capacity <= 2);
    assert_eq!(
        plan.columns
            .iter()
            .find(|column| column.name == "DATA")
            .expect("DATA plan")
            .storage_manager,
        MeasurementSetColumnStorage::TiledShape
    );
    assert_eq!(
        plan.columns
            .iter()
            .find(|column| column.name == "UVW")
            .expect("UVW plan")
            .storage_manager,
        MeasurementSetColumnStorage::TiledColumn
    );
    assert!(
        plan.columns
            .iter()
            .filter(|column| { column.storage_manager == MeasurementSetColumnStorage::Standard })
            .count()
            >= 16
    );
}

#[test]
fn batches_are_whole_tile_bands_near_the_target_however_large_the_budget() {
    // 64 GiB of budget: the batch still follows the target, not memory.
    for channels in [16, 256, 1024, 4096] {
        let plan = MeasurementSetWritePlan::visibility_creation(
            50_000_000,
            2,
            channels,
            "ALMA",
            MeasurementSetWriteResources {
                available_bytes: 64 * 1024 * 1024 * 1024,
                maximum_live_batches: 2,
                tiled_column_buffer_bytes: STREAMING_TILED_COLUMN_BUFFER_BYTES,
            },
        )
        .expect("write plan");
        let tile_rows = plan.visibility_tile_shape[2];
        let band_bytes = tile_rows * plan.bytes_per_row;
        assert!(plan.batch_rows.is_multiple_of(tile_rows), "{channels}");
        // As many bands as fit the target, and at least one.
        assert!(
            plan.batch_bytes <= TARGET_BATCH_BYTES.max(band_bytes),
            "{channels}: {}",
            plan.batch_bytes
        );
        assert!(
            plan.batch_bytes + band_bytes > TARGET_BATCH_BYTES,
            "{channels}: {}",
            plan.batch_bytes
        );
        assert_eq!(plan.queue_capacity, 1, "{channels}");
    }
}

#[test]
fn a_budget_below_one_tile_band_still_plans_rows_that_fit() {
    let plan = MeasurementSetWritePlan::visibility_creation(
        100_000,
        2,
        1024,
        "ALMA",
        MeasurementSetWriteResources {
            available_bytes: 96 * 1024 * 1024,
            maximum_live_batches: 2,
            tiled_column_buffer_bytes: STREAMING_TILED_COLUMN_BUFFER_BYTES,
        },
    )
    .expect("write plan");
    assert!(plan.batch_rows < plan.visibility_tile_shape[2]);
    assert!(plan.maximum_resident_bytes <= 96 * 1024 * 1024);
}

#[test]
fn plan_rejects_one_byte_below_one_row() {
    let row_plan = MeasurementSetWritePlan::visibility_creation(
        1,
        1,
        1,
        "VLA",
        MeasurementSetWriteResources {
            available_bytes: usize::MAX,
            maximum_live_batches: 1,
            tiled_column_buffer_bytes: 0,
        },
    )
    .expect("derive exact row bytes");
    assert_eq!(
        row_plan.data_bands, 2,
        "a large budget overlaps DATA's writes"
    );
    // A byte less gives up the second band of DATA tiles rather than
    // the row, and a byte less than that cannot hold the row.
    let plan = |available_bytes| {
        MeasurementSetWritePlan::visibility_creation(
            1,
            1,
            1,
            "VLA",
            MeasurementSetWriteResources {
                available_bytes,
                maximum_live_batches: 1,
                tiled_column_buffer_bytes: 0,
            },
        )
    };
    let one_band = plan(row_plan.maximum_resident_bytes - 1).expect("one band fits");
    assert_eq!(one_band.data_bands, 1);
    let error = plan(one_band.maximum_resident_bytes - 1).expect_err("one row cannot fit");
    assert!(matches!(
        error,
        MeasurementSetWriteError::InsufficientBudget { .. }
    ));
}

#[test]
fn visibility_plan_charges_every_explicit_column_io_buffer() {
    let plan_without_io_buffers = MeasurementSetWritePlan::visibility_creation(
        0,
        2,
        16,
        "VLA",
        MeasurementSetWriteResources {
            available_bytes: 256 * 1024 * 1024,
            maximum_live_batches: 2,
            tiled_column_buffer_bytes: 0,
        },
    )
    .expect("plan without I/O buffers");
    let per_column_buffer = 4 * 1024;
    let plan_with_io_buffers = MeasurementSetWritePlan::visibility_creation(
        0,
        2,
        16,
        "VLA",
        MeasurementSetWriteResources {
            available_bytes: 256 * 1024 * 1024,
            maximum_live_batches: 2,
            tiled_column_buffer_bytes: per_column_buffer,
        },
    )
    .expect("plan with I/O buffers");

    // DATA, FLAG, UVW, WEIGHT and SIGMA each hold a buffer; FLAG_CATEGORY
    // is written undefined, with no writer.
    assert_eq!(
        plan_with_io_buffers.maximum_resident_bytes
            - plan_without_io_buffers.maximum_resident_bytes,
        5 * per_column_buffer
    );
}

#[test]
fn visibility_session_rejects_a_batch_above_the_planned_boundary() {
    let mut plan = MeasurementSetWritePlan::visibility_creation(
        2,
        1,
        1,
        "VLA",
        MeasurementSetWriteResources {
            available_bytes: 64 * 1024 * 1024,
            maximum_live_batches: 1,
            tiled_column_buffer_bytes: 0,
        },
    )
    .expect("write plan");
    plan.batch_rows = 1;
    plan.batch_bytes = plan.bytes_per_row;
    let (sender, receiver) = mpsc::sync_channel(0);
    let (_returns, recycled) = mpsc::sync_channel(1);
    let session = MeasurementSetWriteSession {
        plan,
        state: MeasurementSetWriteSessionState::Creation {
            output: PathBuf::new(),
            sender,
            handle: thread::spawn(|| {
                Err(MeasurementSetWriteError::Background(
                    "test writer stopped".to_string(),
                ))
            }),
            sent_rows: AtomicUsize::new(0),
            scalar_writers: ScalarWriters::default(),
            queue_wait_nanos: AtomicU64::new(0),
            started_at: Instant::now(),
            recycled,
        },
    };
    let error = session
        .send_visibility_columns(VisibilityColumns {
            data: vec![Complex32::new(1.0, 0.0); 2],
            flag_row: vec![false; 2],
            uvw: vec![[0.0; 3]; 2],
            ..VisibilityColumns::default()
        })
        .expect_err("oversized batch must be rejected");
    assert!(error.to_string().contains("plan permits at most 1"));
    drop(receiver);
}

#[test]
fn columnar_plan_accounts_for_every_scalar_writer_buffer() {
    let scalar_buffer_bytes =
        streamed_scalar_columns_resident_bytes(2).expect("two scalar columns");
    let arrays = vec![MeasurementSetArrayColumnPlan {
        name: "DATA".to_string(),
        value_type: StreamedTiledShapeValueType::Complex32,
        shapes: vec![MeasurementSetArrayShapePlan {
            cell_shape: vec![1, 1],
            row_count: 10,
            tile_shape: vec![1, 1, 10],
        }],
        storage_manager: MeasurementSetColumnStorage::TiledShape,
    }];
    let scalars = vec![
        MeasurementSetScalarColumnPlan {
            name: "FLAG_ROW".to_string(),
            value_type: StreamedScalarType::Bool,
        },
        MeasurementSetScalarColumnPlan {
            name: "TIME".to_string(),
            value_type: StreamedScalarType::Float64,
        },
    ];
    let plan = MeasurementSetWritePlan::variable_array_creation(
        10,
        arrays.clone(),
        scalars.clone(),
        MeasurementSetWriteResources {
            available_bytes: scalar_buffer_bytes + 4 * 1024,
            maximum_live_batches: 2,
            tiled_column_buffer_bytes: 0,
        },
    )
    .expect("columnar plan");
    assert_eq!(plan.bytes_per_row, 8 + 2 + 9);
    assert_eq!(plan.columns.len(), 3);
    assert!(plan.maximum_resident_bytes >= scalar_buffer_bytes);
    assert!(plan.maximum_resident_bytes <= scalar_buffer_bytes + 4 * 1024);

    let error = MeasurementSetWritePlan::variable_array_creation(
        10,
        arrays,
        scalars,
        MeasurementSetWriteResources {
            available_bytes: scalar_buffer_bytes - 1,
            maximum_live_batches: 2,
            tiled_column_buffer_bytes: 0,
        },
    )
    .expect_err("fixed scalar buffers must fit");
    assert!(error.to_string().contains("scalar column buffers require"));
}

#[test]
fn variable_array_plan_requires_one_fixed_shape_for_tiled_column() {
    let resources = MeasurementSetWriteResources {
        available_bytes: 64 * 1024 * 1024,
        maximum_live_batches: 2,
        tiled_column_buffer_bytes: 0,
    };
    let fixed = MeasurementSetArrayColumnPlan {
        name: "UVW".to_string(),
        value_type: StreamedTiledShapeValueType::Float64,
        shapes: vec![MeasurementSetArrayShapePlan {
            cell_shape: vec![3],
            row_count: 10,
            tile_shape: vec![3, 10],
        }],
        storage_manager: MeasurementSetColumnStorage::TiledColumn,
    };

    let plan = MeasurementSetWritePlan::variable_array_creation(
        10,
        vec![fixed.clone()],
        Vec::new(),
        resources,
    )
    .expect("fixed-shape tiled column plan");
    assert_eq!(
        plan.array_columns[0].storage_manager,
        MeasurementSetColumnStorage::TiledColumn
    );

    let mut heterogeneous = fixed;
    heterogeneous.shapes = vec![
        MeasurementSetArrayShapePlan {
            cell_shape: vec![3],
            row_count: 5,
            tile_shape: vec![3, 5],
        },
        MeasurementSetArrayShapePlan {
            cell_shape: vec![4],
            row_count: 5,
            tile_shape: vec![4, 5],
        },
    ];
    let error = MeasurementSetWritePlan::variable_array_creation(
        10,
        vec![heterogeneous],
        Vec::new(),
        resources,
    )
    .expect_err("heterogeneous tiled column must be rejected");
    assert!(
        error
            .to_string()
            .contains("requires one defined shape for every row")
    );
}

#[test]
fn variable_shape_plans_stay_bounded_for_realistic_and_skewed_cells() {
    let cases = [
        ("small", vec![(vec![1, 16], 12_000)]),
        ("medium", vec![(vec![2, 128], 80_000)]),
        ("large", vec![(vec![4, 4_096], 25_000)]),
        (
            "skewed",
            vec![
                (vec![1, 1], 90_000),
                (vec![2, 64], 9_900),
                (vec![4, 8_192], 100),
            ],
        ),
    ];
    let available_bytes = 256 * 1024 * 1024;

    for (name, shapes) in cases {
        let row_count = shapes.iter().map(|(_, rows)| rows).sum();
        let shapes = shapes
            .into_iter()
            .map(|(cell_shape, row_count)| MeasurementSetArrayShapePlan {
                tile_shape: MeasurementSetArrayShapePlan::visibility(
                    cell_shape[0],
                    cell_shape[1],
                    row_count,
                    "VLA",
                )
                .tile_shape,
                cell_shape,
                row_count,
            })
            .collect();
        let plan = MeasurementSetWritePlan::variable_array_creation(
            row_count,
            vec![MeasurementSetArrayColumnPlan {
                name: "DATA".to_string(),
                value_type: StreamedTiledShapeValueType::Complex32,
                shapes,
                storage_manager: MeasurementSetColumnStorage::TiledShape,
            }],
            standard_main_scalar_column_plans(),
            MeasurementSetWriteResources {
                available_bytes,
                maximum_live_batches: 3,
                tiled_column_buffer_bytes: 0,
            },
        )
        .unwrap_or_else(|error| panic!("{name} shape plan failed: {error}"));

        assert!(plan.batch_rows > 0, "{name}");
        assert!(
            plan.maximum_resident_bytes <= available_bytes,
            "{name}: {} > {available_bytes}",
            plan.maximum_resident_bytes
        );
    }
}

#[test]
fn create_target_preserves_existing_output_until_commit() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let final_path = directory.path().join("output.ms");
    fs::create_dir(&final_path).expect("existing output");
    fs::write(final_path.join("sentinel"), b"complete").expect("sentinel");

    let target = MeasurementSetCreateTarget::prepare(&final_path, true).expect("target");
    assert!(final_path.join("sentinel").exists());
    fs::create_dir(target.staging_path()).expect("staging output");
    fs::write(target.staging_path().join("replacement"), b"complete").expect("replacement");
    target.commit().expect("publish replacement");

    assert!(!final_path.join("sentinel").exists());
    assert!(final_path.join("replacement").exists());
}

#[test]
fn uncommitted_create_target_is_detectably_incomplete() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let final_path = directory.path().join("output.ms");
    let target = MeasurementSetCreateTarget::prepare(&final_path, false).expect("target");
    fs::create_dir(target.staging_path()).expect("staging output");

    assert!(!final_path.exists());
    assert!(
        target
            .staging_path()
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.contains("casa-rs-incomplete"))
    );
}
