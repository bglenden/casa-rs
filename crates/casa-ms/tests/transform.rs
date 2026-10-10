// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use casa_ms::{
    MeasurementSet, MeasurementSetColumnStorage, MeasurementSetColumnWriteMode,
    MeasurementSetMutationBatch, MeasurementSetMutationColumnBatch,
    MeasurementSetMutationColumnValues, MeasurementSetWriteColumnPlan, MeasurementSetWritePlan,
    MeasurementSetWriteResources, MeasurementSetWriteSession, MsTransformRequest, SubTable,
    TransformDataColumn, mstransform, schema::main_table::VisibilityDataColumn,
    selection::MsSelection,
};
use casa_types::{ArrayValue, ScalarValue};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

#[test]
fn mstransform_selects_channels_updates_metadata_and_weight_spectrum() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input_ms = common::create_msexplore_spectrum_fixture_ms(dir.path(), true, &[]);
    let output_ms = dir.path().join("transformed.ms");

    let report = mstransform(&MsTransformRequest {
        input_ms: input_ms.clone(),
        output_ms: output_ms.clone(),
        spw: "0:2~5".to_string(),
        width: 1,
        data_column: TransformDataColumn::Data,
        selection: MsSelection::new(),
        keep_flags: true,
    })
    .expect("mstransform");

    assert_eq!(report.row_count, 4);
    assert_eq!(report.source_column, "DATA");
    assert_eq!(report.output_column, "DATA");
    assert_eq!(report.spectral_window_ids, vec![0]);
    assert_eq!(report.output_channels_by_spw[&0], 4);

    let output = MeasurementSet::open(&output_ms).expect("open transformed MS");
    assert_eq!(output.row_count(), 4);
    let data = output
        .data_column(VisibilityDataColumn::Data)
        .expect("DATA column");
    let row = data.get(1).expect("row 1 DATA");
    let ArrayValue::Complex32(row) = row else {
        panic!("expected Complex32 DATA");
    };
    assert_eq!(row.shape(), &[common::NUM_CORR, 4]);
    for corr in 0..common::NUM_CORR {
        for (out_chan, source_chan) in (2..=5).enumerate() {
            let expected_index =
                common::NUM_CORR * common::NUM_CHAN + source_chan * common::NUM_CORR + corr;
            assert_eq!(
                row[[corr, out_chan]],
                casa_types::Complex32::new(expected_index as f32, -(expected_index as f32) * 0.5)
            );
        }
    }

    let flags = output
        .main_table()
        .cell_accessor(0, "FLAG")
        .and_then(|cell| cell.array())
        .expect("FLAG row");
    assert_eq!(flags.shape(), &[common::NUM_CORR, 4]);
    let weights = output
        .main_table()
        .cell_accessor(1, "WEIGHT_SPECTRUM")
        .and_then(|cell| cell.array())
        .expect("WEIGHT_SPECTRUM row");
    let ArrayValue::Float32(weights) = weights else {
        panic!("expected Float32 WEIGHT_SPECTRUM");
    };
    assert_eq!(weights.shape(), &[common::NUM_CORR, 4]);
    assert_eq!(weights[[0, 0]], 2008.0);
    assert_eq!(weights[[3, 3]], 2107.0);

    let spw = output.spectral_window().expect("SPECTRAL_WINDOW");
    assert_eq!(spw.num_chan(0).expect("NUM_CHAN"), 4);
    assert_eq!(
        spw.chan_freq(0).expect("CHAN_FREQ"),
        vec![1.002e9, 1.003e9, 1.004e9, 1.005e9]
    );
    let spw_table = spw.table();
    assert_eq!(
        spw_table
            .cell_accessor(0, "REF_FREQUENCY")
            .and_then(|cell| cell.scalar())
            .expect("REF_FREQUENCY"),
        &ScalarValue::Float64(1.002e9)
    );
    assert_eq!(
        spw_table
            .cell_accessor(0, "TOTAL_BANDWIDTH")
            .and_then(|cell| cell.scalar())
            .expect("TOTAL_BANDWIDTH"),
        &ScalarValue::Float64(4.0e6)
    );
}

#[test]
fn mstransform_filters_rows_by_selected_spw_and_preserves_time_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input_ms = common::create_msexplore_averaging_fixture_ms(dir.path());
    let output_ms = dir.path().join("spw1.ms");

    let report = mstransform(&MsTransformRequest {
        input_ms: input_ms.clone(),
        output_ms: output_ms.clone(),
        spw: "1:0~5".to_string(),
        width: 1,
        data_column: TransformDataColumn::Data,
        selection: MsSelection::new().field(&[0]),
        keep_flags: true,
    })
    .expect("mstransform spw 1");

    assert_eq!(report.row_count, 3);
    assert_eq!(report.spectral_window_ids, vec![1]);
    assert_eq!(report.output_channels_by_spw[&1], 6);

    let output = MeasurementSet::open(&output_ms).expect("open transformed MS");
    let data = output
        .data_column(VisibilityDataColumn::Data)
        .expect("DATA column");
    for row_index in 0..output.row_count() {
        assert_eq!(data.shape(row_index).expect("DATA shape"), vec![4, 6]);
        assert_eq!(
            output
                .main_table()
                .cell_accessor(row_index, "DATA_DESC_ID")
                .and_then(|cell| cell.scalar())
                .expect("DATA_DESC_ID"),
            &ScalarValue::Int32(0)
        );
    }
    let spw = output.spectral_window().expect("SPECTRAL_WINDOW");
    assert_eq!(spw.row_count(), 1);
    assert_eq!(spw.num_chan(0).expect("NUM_CHAN"), 6);
    let data_description = output.data_description().expect("DATA_DESCRIPTION");
    assert_eq!(data_description.row_count(), 1);
    assert_eq!(
        data_description
            .spectral_window_id(0)
            .expect("compact DATA_DESCRIPTION SPW"),
        0
    );
    assert_eq!(
        output
            .main_table()
            .cell_accessor(0, "TIME")
            .and_then(|cell| cell.scalar())
            .expect("first TIME"),
        &ScalarValue::Float64(common::TIME_BASE_SECONDS)
    );
    assert_eq!(
        output
            .main_table()
            .cell_accessor(1, "TIME")
            .and_then(|cell| cell.scalar())
            .expect("second TIME"),
        &ScalarValue::Float64(common::TIME_BASE_SECONDS)
    );
    assert_eq!(
        output
            .main_table()
            .cell_accessor(2, "TIME")
            .and_then(|cell| cell.scalar())
            .expect("third TIME"),
        &ScalarValue::Float64(common::TIME_BASE_SECONDS + 30.0)
    );
}

#[test]
fn mstransform_compacts_selected_field_table_and_remaps_main_field_ids() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input_ms = common::create_msexplore_averaging_fixture_ms(dir.path());
    let output_ms = dir.path().join("field1.ms");

    let report = mstransform(&MsTransformRequest {
        input_ms: input_ms.clone(),
        output_ms: output_ms.clone(),
        spw: "0".to_string(),
        width: 1,
        data_column: TransformDataColumn::Data,
        selection: MsSelection::new().field(&[1]),
        keep_flags: true,
    })
    .expect("mstransform field 1");

    assert_eq!(report.row_count, 1);
    let output = MeasurementSet::open(&output_ms).expect("open transformed MS");
    assert_eq!(output.row_count(), 1);
    let field = output.field().expect("FIELD");
    assert_eq!(field.row_count(), 1);
    assert_eq!(field.name(0).expect("field name"), "FIELD1");
    assert_eq!(
        output
            .main_table()
            .cell_accessor(0, "FIELD_ID")
            .and_then(|cell| cell.scalar())
            .expect("FIELD_ID"),
        &ScalarValue::Int32(0)
    );
}

#[test]
fn mstransform_width_averages_selected_channels_and_metadata() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input_ms = common::create_msexplore_spectrum_fixture_ms(dir.path(), true, &[]);
    let output_ms = dir.path().join("averaged.ms");

    let report = mstransform(&MsTransformRequest {
        input_ms: input_ms.clone(),
        output_ms: output_ms.clone(),
        spw: "0:2~5".to_string(),
        width: 2,
        data_column: TransformDataColumn::Data,
        selection: MsSelection::new(),
        keep_flags: true,
    })
    .expect("mstransform width");

    assert_eq!(report.output_channels_by_spw[&0], 2);
    assert_eq!(report.width, 2);

    let output = MeasurementSet::open(&output_ms).expect("open averaged MS");
    let data = output
        .data_column(VisibilityDataColumn::Data)
        .expect("DATA column");
    let row = data.get(1).expect("row 1 DATA");
    let ArrayValue::Complex32(row) = row else {
        panic!("expected Complex32 DATA");
    };
    assert_eq!(row.shape(), &[common::NUM_CORR, 2]);
    for corr in 0..common::NUM_CORR {
        let source_index_2 = common::NUM_CORR * common::NUM_CHAN + 2 * common::NUM_CORR + corr;
        let source_index_3 = common::NUM_CORR * common::NUM_CHAN + 3 * common::NUM_CORR + corr;
        let expected = ((source_index_2 + source_index_3) as f32) / 2.0;
        assert_eq!(
            row[[corr, 0]],
            casa_types::Complex32::new(expected, -expected * 0.5)
        );
    }

    let spw = output.spectral_window().expect("SPECTRAL_WINDOW");
    assert_eq!(spw.num_chan(0).expect("NUM_CHAN"), 2);
    assert_eq!(
        spw.chan_freq(0).expect("CHAN_FREQ"),
        vec![1.0025e9, 1.0045e9]
    );
    let spw_table = spw.table();
    let widths = spw_table
        .cell_accessor(0, "CHAN_WIDTH")
        .and_then(|cell| cell.array())
        .expect("CHAN_WIDTH");
    let ArrayValue::Float64(widths) = widths else {
        panic!("expected Float64 CHAN_WIDTH");
    };
    assert_eq!(
        widths.iter().copied().collect::<Vec<_>>(),
        vec![2.0e6, 2.0e6]
    );
    assert_eq!(
        spw_table
            .cell_accessor(0, "TOTAL_BANDWIDTH")
            .and_then(|cell| cell.scalar())
            .expect("TOTAL_BANDWIDTH"),
        &ScalarValue::Float64(4.0e6)
    );
}

#[test]
fn mstransform_no_keepflags_drops_flag_row_samples() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input_ms = common::create_msexplore_spectrum_fixture_ms(dir.path(), true, &[1, 3]);
    let output_ms = dir.path().join("unflagged.ms");

    let report = mstransform(&MsTransformRequest {
        input_ms: input_ms.clone(),
        output_ms: output_ms.clone(),
        spw: "0:0~3".to_string(),
        width: 1,
        data_column: TransformDataColumn::Data,
        selection: MsSelection::new(),
        keep_flags: false,
    })
    .expect("mstransform keepflags false");

    assert_eq!(report.row_count, 2);
    let output = MeasurementSet::open(&output_ms).expect("open transformed MS");
    assert_eq!(output.row_count(), 2);
    assert_eq!(
        output
            .main_table()
            .cell_accessor(0, "SCAN_NUMBER")
            .and_then(|cell| cell.scalar())
            .expect("first scan"),
        &ScalarValue::Int32(1)
    );
    assert_eq!(
        output
            .main_table()
            .cell_accessor(1, "SCAN_NUMBER")
            .and_then(|cell| cell.scalar())
            .expect("second scan"),
        &ScalarValue::Int32(3)
    );
}

#[test]
fn mstransform_reports_contract_errors_before_mutating_outputs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input_ms = common::create_msexplore_fixture_ms(dir.path());
    let output_ms = dir.path().join("bad.ms");

    let same_path = mstransform(&MsTransformRequest {
        input_ms: input_ms.clone(),
        output_ms: input_ms.clone(),
        spw: "0".to_string(),
        width: 1,
        data_column: TransformDataColumn::Data,
        selection: MsSelection::new(),
        keep_flags: true,
    })
    .unwrap_err();
    assert!(same_path.to_string().contains("must differ"));

    let invalid_spw = mstransform(&MsTransformRequest {
        input_ms: input_ms.clone(),
        output_ms: output_ms.clone(),
        spw: "9".to_string(),
        width: 1,
        data_column: TransformDataColumn::Data,
        selection: MsSelection::new(),
        keep_flags: true,
    })
    .unwrap_err();
    assert!(invalid_spw.to_string().contains("outside SPECTRAL_WINDOW"));
    assert!(!output_ms.exists());

    let missing_column = mstransform(&MsTransformRequest {
        input_ms,
        output_ms,
        spw: "0".to_string(),
        width: 1,
        data_column: TransformDataColumn::CorrectedData,
        selection: MsSelection::new(),
        keep_flags: true,
    })
    .unwrap_err();
    assert!(missing_column.to_string().contains("CORRECTED_DATA"));
}

#[test]
fn selected_row_write_session_persists_bounded_typed_batches() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ms_path = common::create_msexplore_spectrum_fixture_ms(dir.path(), true, &[]);
    let mut measurement_set = MeasurementSet::open(&ms_path).expect("open fixture MS");
    let samples = common::NUM_CORR * common::NUM_CHAN;
    let selected_rows = vec![0, 2];
    let plan = MeasurementSetWritePlan::selected_row_mutation(
        selected_rows.clone(),
        vec![
            MeasurementSetWriteColumnPlan {
                name: "FLAG".to_string(),
                bytes_per_row: samples,
                mode: MeasurementSetColumnWriteMode::Replace,
                storage_manager: MeasurementSetColumnStorage::Persisted,
                tile_shape: None,
                create_source_column: None,
            },
            MeasurementSetWriteColumnPlan {
                name: "FLAG_ROW".to_string(),
                bytes_per_row: 1,
                mode: MeasurementSetColumnWriteMode::Replace,
                storage_manager: MeasurementSetColumnStorage::Persisted,
                tile_shape: None,
                create_source_column: None,
            },
        ],
        MeasurementSetWriteResources {
            available_bytes: 2 * (samples + 1),
            maximum_live_batches: 2,
            tiled_column_buffer_bytes: 0,
        },
    )
    .expect("mutation plan");
    assert_eq!(plan.batch_rows(), 1);

    let mut session =
        MeasurementSetWriteSession::start_selected_row_mutation(&mut measurement_set, plan)
            .expect("start mutation");
    while !session
        .next_mutation_rows()
        .expect("next mutation rows")
        .is_empty()
    {
        let rows = session
            .next_mutation_rows()
            .expect("next mutation rows")
            .to_vec();
        session
            .write_mutation_batch(
                &mut measurement_set,
                MeasurementSetMutationBatch {
                    row_indices: rows,
                    columns: vec![
                        MeasurementSetMutationColumnBatch {
                            name: "FLAG".to_string(),
                            values: MeasurementSetMutationColumnValues::Arrays(vec![
                                ArrayValue::Bool(
                                    ArrayD::from_shape_vec(
                                        IxDyn(&[common::NUM_CORR, common::NUM_CHAN]).f(),
                                        vec![true; samples],
                                    )
                                    .expect("FLAG cell"),
                                ),
                            ]),
                        },
                        MeasurementSetMutationColumnBatch {
                            name: "FLAG_ROW".to_string(),
                            values: MeasurementSetMutationColumnValues::Scalars(vec![
                                ScalarValue::Bool(true),
                            ]),
                        },
                    ],
                },
            )
            .expect("write mutation batch");
    }
    let telemetry = session.finish_mutation().expect("finish mutation");
    assert_eq!(telemetry.bytes_written, selected_rows.len() * (samples + 1));
    assert_eq!(telemetry.rows_written, selected_rows.len());
    assert_eq!(telemetry.maximum_resident_bytes, 2 * (samples + 1));
    assert_eq!(telemetry.queue_wait_seconds, 0.0);
    assert!(telemetry.producer_seconds >= telemetry.write_seconds);
    assert!(telemetry.finalize_seconds >= 0.0);
    drop(measurement_set);
    let entries = std::fs::read_dir(&ms_path)
        .expect("list mutated MS")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect::<Vec<_>>();
    assert!(
        entries.iter().all(|name| !name.contains("casa-rs")),
        "an in-place mutation left casa-rs files: {entries:?}"
    );

    let reopened = MeasurementSet::open(&ms_path).expect("reopen mutated MS");
    for row in selected_rows {
        let flags = reopened
            .main_table()
            .cell_accessor(row, "FLAG")
            .and_then(|cell| cell.array())
            .expect("mutated FLAG");
        let ArrayValue::Bool(flags) = flags else {
            panic!("expected Bool FLAG");
        };
        assert!(flags.iter().all(|flag| *flag));
        assert_eq!(
            reopened
                .main_table()
                .cell_accessor(row, "FLAG_ROW")
                .and_then(|cell| cell.scalar())
                .expect("mutated FLAG_ROW"),
            &ScalarValue::Bool(true)
        );
    }
    assert_eq!(
        reopened
            .main_table()
            .cell_accessor(1, "FLAG_ROW")
            .and_then(|cell| cell.scalar())
            .expect("untouched FLAG_ROW"),
        &ScalarValue::Bool(false)
    );
}

#[test]
fn interrupted_selected_row_write_keeps_persisted_rows_and_reopens() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ms_path = common::create_msexplore_spectrum_fixture_ms(dir.path(), true, &[]);
    let mut measurement_set = MeasurementSet::open(&ms_path).expect("open fixture MS");
    let plan = MeasurementSetWritePlan::selected_row_mutation(
        vec![0],
        vec![MeasurementSetWriteColumnPlan {
            name: "FLAG_ROW".to_string(),
            bytes_per_row: 1,
            mode: MeasurementSetColumnWriteMode::Replace,
            storage_manager: MeasurementSetColumnStorage::Persisted,
            tile_shape: None,
            create_source_column: None,
        }],
        MeasurementSetWriteResources {
            available_bytes: 1,
            maximum_live_batches: 1,
            tiled_column_buffer_bytes: 0,
        },
    )
    .expect("mutation plan");
    let mut session =
        MeasurementSetWriteSession::start_selected_row_mutation(&mut measurement_set, plan)
            .expect("start mutation");
    session
        .write_mutation_batch(
            &mut measurement_set,
            MeasurementSetMutationBatch {
                row_indices: vec![0],
                columns: vec![MeasurementSetMutationColumnBatch {
                    name: "FLAG_ROW".to_string(),
                    values: MeasurementSetMutationColumnValues::Scalars(vec![ScalarValue::Bool(
                        true,
                    )]),
                }],
            },
        )
        .expect("persist one row before interruption");
    drop(session);
    drop(measurement_set);

    // As after an interrupted CASA write, the MeasurementSet reopens with the
    // rows persisted before the interruption.
    let reopened = MeasurementSet::open(&ms_path).expect("reopen after an interrupted write");
    assert_eq!(
        reopened
            .main_table()
            .cell_accessor(0, "FLAG_ROW")
            .and_then(|cell| cell.scalar())
            .expect("persisted interrupted value"),
        &ScalarValue::Bool(true)
    );
}

/// Start a one-row `FLAG_ROW` mutation on `measurement_set`.
fn start_flag_row_mutation(
    measurement_set: &mut MeasurementSet,
) -> Result<MeasurementSetWriteSession, casa_ms::MeasurementSetWriteError> {
    let plan = MeasurementSetWritePlan::selected_row_mutation(
        vec![0],
        vec![MeasurementSetWriteColumnPlan {
            name: "FLAG_ROW".to_string(),
            bytes_per_row: 1,
            mode: MeasurementSetColumnWriteMode::Replace,
            storage_manager: MeasurementSetColumnStorage::Persisted,
            tile_shape: None,
            create_source_column: None,
        }],
        MeasurementSetWriteResources {
            available_bytes: 1,
            maximum_live_batches: 1,
            tiled_column_buffer_bytes: 0,
        },
    )
    .expect("mutation plan");
    MeasurementSetWriteSession::start_selected_row_mutation(measurement_set, plan)
}

/// Write the one planned `FLAG_ROW` row and complete the session.
fn finish_flag_row_mutation(
    mut session: MeasurementSetWriteSession,
    measurement_set: &mut MeasurementSet,
) {
    session
        .write_mutation_batch(
            measurement_set,
            MeasurementSetMutationBatch {
                row_indices: vec![0],
                columns: vec![MeasurementSetMutationColumnBatch {
                    name: "FLAG_ROW".to_string(),
                    values: MeasurementSetMutationColumnValues::Scalars(vec![ScalarValue::Bool(
                        true,
                    )]),
                }],
            },
        )
        .expect("write FLAG_ROW");
    session.finish_mutation().expect("finish mutation");
}

const WRITE_LOCK_PROBE_TABLE: &str = "CASA_RS_WRITE_LOCK_PROBE_TABLE";
const WRITE_LOCK_HOLD_SIGNAL: &str = "CASA_RS_WRITE_LOCK_HOLD_SIGNAL";
const WRITE_LOCK_HOLD_RELEASE: &str = "CASA_RS_WRITE_LOCK_HOLD_RELEASE";

/// Child-process half of the write-lock tests: tries casacore's write lock on
/// the table named by the environment once, as another process would, and
/// reports the outcome. With a signal and a release file named too, it holds
/// the lock, creates the signal file, and releases once the release file
/// appears (or after 20 s). Without the environment it does nothing.
#[test]
fn write_lock_probe_from_another_process() {
    let Some(table) = std::env::var_os(WRITE_LOCK_PROBE_TABLE) else {
        return;
    };
    let lock = match casa_tables::TableWriteLock::acquire(&table, 1) {
        Ok(lock) => lock,
        Err(error) => {
            println!("write-lock-probe: refused ({error})");
            return;
        }
    };
    println!("write-lock-probe: acquired");
    if let (Some(signal), Some(release)) = (
        std::env::var_os(WRITE_LOCK_HOLD_SIGNAL),
        std::env::var_os(WRITE_LOCK_HOLD_RELEASE),
    ) {
        std::fs::write(signal, "locked").expect("signal the held lock");
        let release = std::path::PathBuf::from(release);
        let start = std::time::Instant::now();
        while !release.exists() && start.elapsed() < std::time::Duration::from_secs(20) {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }
    drop(lock);
}

/// Whether another process can take casacore's write lock on `table`.
fn another_process_takes_the_write_lock(table: &std::path::Path) -> bool {
    let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "write_lock_probe_from_another_process",
            "--exact",
            "--nocapture",
        ])
        .env(WRITE_LOCK_PROBE_TABLE, table)
        .output()
        .expect("run the write-lock probe");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains("write-lock-probe: "),
        "the write-lock probe did not run: {stdout} {}",
        String::from_utf8_lossy(&output.stderr)
    );
    stdout.contains("write-lock-probe: acquired")
}

/// An in-place mutation holds casacore's write lock on MAIN from start to
/// completion. Until the first completes, another handle in this process is
/// refused at once and another process cannot take the lock (one attempt
/// fails), and opening and closing other handles on the MeasurementSet
/// meanwhile does not drop it.
#[test]
fn selected_row_mutation_holds_the_table_write_lock_until_it_completes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ms_path = common::create_msexplore_spectrum_fixture_ms(dir.path(), true, &[]);
    let mut first = MeasurementSet::open(&ms_path).expect("open first handle");
    let mut second = MeasurementSet::open(&ms_path).expect("open second handle");

    let session = start_flag_row_mutation(&mut first).expect("first session starts");
    let refused = start_flag_row_mutation(&mut second);
    match &refused {
        Err(casa_ms::MeasurementSetWriteError::WriteLock {
            source: casa_tables::TableError::LockFailed { message, .. },
            ..
        }) => assert!(message.contains("in this process"), "{message}"),
        _ => panic!(
            "a second in-place writer was admitted while the first session was live: {:?}",
            refused.err()
        ),
    }
    assert!(matches!(
        second.save_main_table_only(),
        Err(casa_ms::MsError::Table(
            casa_tables::TableError::LockFailed { .. }
        ))
    ));
    drop(MeasurementSet::open(&ms_path).expect("open and close a third handle"));
    assert!(
        !another_process_takes_the_write_lock(&ms_path),
        "another process took MAIN's write lock during the session"
    );

    finish_flag_row_mutation(session, &mut first);
    assert!(another_process_takes_the_write_lock(&ms_path));
    let session = start_flag_row_mutation(&mut second)
        .expect("a new session is admitted once the first completes");
    finish_flag_row_mutation(session, &mut second);
}

/// A session dropped before completion releases the lock, and the rows it
/// persisted stay written.
#[test]
fn an_abandoned_selected_row_mutation_releases_the_table_write_lock() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ms_path = common::create_msexplore_spectrum_fixture_ms(dir.path(), true, &[]);
    let mut first = MeasurementSet::open(&ms_path).expect("open first handle");
    let mut second = MeasurementSet::open(&ms_path).expect("open second handle");

    let session = start_flag_row_mutation(&mut first).expect("first session starts");
    drop(session);
    let session = start_flag_row_mutation(&mut second)
        .expect("a new session is admitted once the first is abandoned");
    finish_flag_row_mutation(session, &mut second);
}

/// Hold casacore's write lock on `table` in another process until
/// `release` appears; returns once the lock is held.
fn hold_the_write_lock_in_another_process(
    table: &std::path::Path,
    signal: &std::path::Path,
    release: &std::path::Path,
) -> std::process::Child {
    let holder = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "write_lock_probe_from_another_process",
            "--exact",
            "--nocapture",
        ])
        .env(WRITE_LOCK_PROBE_TABLE, table)
        .env(WRITE_LOCK_HOLD_SIGNAL, signal)
        .env(WRITE_LOCK_HOLD_RELEASE, release)
        .stdout(std::process::Stdio::null())
        .spawn()
        .expect("start the lock holder");
    let start = std::time::Instant::now();
    while !signal.exists() {
        assert!(
            start.elapsed() < std::time::Duration::from_secs(20),
            "the other process did not take the write lock"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    holder
}

/// The process ids in the request list of `table`'s `table.lock`, read as
/// casacore's `LockFile` reads it: a big-endian count, then `(pid, hostid)`
/// pairs. A casacore holder using AutoLocking releases its lock when the
/// count is not zero.
///
/// Reading through a separate descriptor and closing it drops every `fcntl`
/// lock this process holds on the file, so it is read only while this
/// process waits for the lock, before the holder is told to release.
fn requesting_pids(table: &std::path::Path) -> Vec<i32> {
    let bytes = std::fs::read(table.join("table.lock")).unwrap_or_default();
    let int = |offset: usize| {
        bytes
            .get(offset..offset + 4)
            .map(|word| i32::from_be_bytes(word.try_into().expect("four bytes")))
    };
    let count = int(0).unwrap_or(0).clamp(0, 32) as usize;
    (0..count).filter_map(|slot| int(4 + 8 * slot)).collect()
}

/// Wait until this process is in the request list of `table`'s lock file.
fn this_process_requests_the_lock(table: &std::path::Path) -> bool {
    let pid = std::process::id() as i32;
    let start = std::time::Instant::now();
    while start.elapsed() < std::time::Duration::from_secs(10) {
        if requesting_pids(table).contains(&pid) {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    false
}

/// An in-place mutation waits, as casacore does, while another process holds
/// MAIN's write lock: it is in MAIN's request list, where a casacore holder
/// using AutoLocking sees it and releases, and it starts once the holder
/// releases.
#[test]
fn selected_row_mutation_waits_for_another_process_holding_main() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ms_path = common::create_msexplore_spectrum_fixture_ms(dir.path(), true, &[]);
    let signal = dir.path().join("holder-locked.signal");
    let release = dir.path().join("holder-release.signal");
    let mut measurement_set = MeasurementSet::open(&ms_path).expect("open MeasurementSet");
    let mut holder = hold_the_write_lock_in_another_process(&ms_path, &signal, &release);

    let releaser = {
        let ms_path = ms_path.clone();
        std::thread::spawn(move || {
            let requested = this_process_requests_the_lock(&ms_path);
            std::fs::write(&release, "release").expect("release the holder");
            requested
        })
    };
    let session = start_flag_row_mutation(&mut measurement_set);
    let requested = releaser.join().expect("releaser thread");
    assert!(holder.wait().expect("holder exits").success());

    assert!(
        requested,
        "the waiting writer was not in MAIN's request list"
    );
    let session = session.expect("the mutation starts once the other process releases");
    finish_flag_row_mutation(session, &mut measurement_set);
    assert!(requesting_pids(&ms_path).is_empty());
}

/// A save waiting for a subtable another process holds does not hold MAIN
/// meanwhile, so it cannot deadlock with a process that holds the subtable
/// while it waits for MAIN; it completes once the subtable is released.
#[test]
fn an_in_place_save_waits_for_a_held_subtable_without_holding_main() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ms_path = common::create_msexplore_spectrum_fixture_ms(dir.path(), true, &[]);
    let antenna = ms_path.join("ANTENNA");
    let signal = dir.path().join("holder-locked.signal");
    let release = dir.path().join("holder-release.signal");
    let mut measurement_set = MeasurementSet::open(&ms_path).expect("open MeasurementSet");
    let mut holder = hold_the_write_lock_in_another_process(&antenna, &signal, &release);

    let releaser = {
        let ms_path = ms_path.clone();
        std::thread::spawn(move || {
            let requested = this_process_requests_the_lock(&antenna);
            let main_free = another_process_takes_the_write_lock(&ms_path);
            std::fs::write(&release, "release").expect("release the holder");
            (requested, main_free)
        })
    };
    let saved = measurement_set.save();
    let (requested, main_free) = releaser.join().expect("releaser thread");
    assert!(holder.wait().expect("holder exits").success());

    assert!(requested, "the save was not in ANTENNA's request list");
    assert!(main_free, "the save held MAIN while it waited for ANTENNA");
    saved.expect("the save completes once ANTENNA is released");
}

/// FLAG_ROW of row 0 in the table at `table`, read without locking.
fn flag_row_0(table: &std::path::Path) -> bool {
    let table = casa_tables::Table::open(casa_tables::TableOptions::new(table))
        .expect("open the flag version");
    match table
        .cell_accessor(0, "FLAG_ROW")
        .and_then(|cell| cell.scalar())
        .expect("FLAG_ROW")
    {
        ScalarValue::Bool(value) => *value,
        other => panic!("unexpected FLAG_ROW {other:?}"),
    }
}

/// Saving into an existing flag version is an in-place write: it holds the
/// version table's write lock from before it reads the version until the
/// version is saved. While another process holds the lock, the save waits in
/// the request list and the version is unchanged; it completes once the
/// lock is released.
#[test]
fn saving_into_an_existing_flag_version_waits_for_its_write_lock() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ms_path = common::create_msexplore_spectrum_fixture_ms(dir.path(), true, &[]);
    let mut measurement_set = MeasurementSet::open(&ms_path).expect("open MeasurementSet");
    casa_ms::save_flag_version(&measurement_set, "v1", "seed", casa_ms::FlagMerge::Replace)
        .expect("create the flag version");
    let version = std::path::PathBuf::from(format!("{}.flagversions/flags.v1", ms_path.display()));
    let saved_flag_row = flag_row_0(&version);
    measurement_set
        .main_table_mut()
        .cell_accessor_mut(0, "FLAG_ROW")
        .expect("FLAG_ROW cell")
        .set(casa_types::Value::Scalar(ScalarValue::Bool(
            !saved_flag_row,
        )))
        .expect("change FLAG_ROW");

    let signal = dir.path().join("holder-locked.signal");
    let release = dir.path().join("holder-release.signal");
    let mut holder = hold_the_write_lock_in_another_process(&version, &signal, &release);
    let releaser = {
        let version = version.clone();
        std::thread::spawn(move || {
            let requested = this_process_requests_the_lock(&version);
            let unchanged = flag_row_0(&version) == saved_flag_row;
            std::fs::write(&release, "release").expect("release the holder");
            (requested, unchanged)
        })
    };
    let saved = casa_ms::save_flag_version(
        &measurement_set,
        "v1",
        "replace",
        casa_ms::FlagMerge::Replace,
    );
    let (requested, unchanged) = releaser.join().expect("releaser thread");
    assert!(holder.wait().expect("holder exits").success());

    assert!(requested, "the save was not in the version's request list");
    assert!(
        unchanged,
        "the version changed while another process held it"
    );
    saved.expect("the save completes once the version is released");
    assert_eq!(flag_row_0(&version), !saved_flag_row);
}
