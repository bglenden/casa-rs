// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::measures::MjdHighPrec;
use casa_core::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use casa_ms::observation_io::{Msv2Observation, OpenOptions, PartitionScheme};
use casa_ms::{MeasurementSet, OptionalMainColumn, SubtableId, VisibilityDataColumn};
use casa_observation::{
    AccessRequest, BatchBuffers, ObservationError, PartitionId, ProcessorId, ReadEvent,
};
use casa_tables::Table;

use super::{fixture, request, split_sessions, three_scans};

fn set_key(main: &mut Table, row: usize, column: &str, value: i32) {
    main.row_accessor_mut()
        .set_cell(row, column, Value::Scalar(ScalarValue::Int32(value)))
        .expect(column);
}

fn add_field(ms: &mut MeasurementSet) {
    let fields = ms.subtable_mut(SubtableId::Field).expect("FIELD");
    assert_eq!(fields.row_count(), 1);
    let field = fields.row_accessor().row(0).expect("first field").clone();
    fields.add_row(field).expect("second field");
}

fn add_processors(ms: &mut MeasurementSet) {
    let processors = ms.subtable_mut(SubtableId::Processor).expect("PROCESSOR");
    assert_eq!(processors.row_count(), 0);
    for id in [0, 1] {
        processors
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "TYPE",
                    Value::Scalar(ScalarValue::String("CORRELATOR".into())),
                ),
                RecordField::new(
                    "SUB_TYPE",
                    Value::Scalar(ScalarValue::String("SYNTHETIC".into())),
                ),
                RecordField::new("MODE_ID", Value::Scalar(ScalarValue::Int32(id))),
                RecordField::new("TYPE_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ]))
            .expect("processor row");
    }
    for row in 0..ms.row_count() {
        set_key(ms.main_table_mut(), row, "PROCESSOR_ID", 0);
    }
}

#[test]
fn same_grid_cell_is_refused() {
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ms");
    let mut ms = MeasurementSet::open(&path).expect("fixture");
    let row = ms
        .main_table()
        .row_accessor()
        .row(0)
        .expect("first row")
        .clone();
    let duplicate = ms.row_count();
    ms.main_table_mut().add_row(row).expect("duplicate");
    ms.save().expect("save");
    drop(ms);
    let error = Msv2Observation::open(path).err().expect("grid refusal");
    eprintln!("{error:?}");
    assert!(matches!(error, ObservationError::InvalidStore { reason }
        if reason.contains(&format!("rows 0 and {duplicate}"))
            && reason.contains("no axis for repeated acquisitions")));
}

/// The matching baseline forces a cell collision, while moving a
/// different baseline to the same field, scan or processor also exercises
/// a conflict without a cell collision. The extra row arrives out of TIME
/// order and has different DATA from the original at that cell.
#[test]
fn distinct_grid_keys_at_one_time_keep_every_acquisition() {
    for (column, setup, new_value) in [
        ("FIELD_ID", add_field as fn(&mut MeasurementSet), 1),
        ("SCAN_NUMBER", |_| {}, 2),
        ("PROCESSOR_ID", add_processors, 1),
    ] {
        let root = tempfile::tempdir().expect("root");
        let path = fixture(root.path(), "ms_trial.ms");
        let mut ms = MeasurementSet::open(&path).expect("fixture");
        setup(&mut ms);
        let extra = ms.row_count();
        let main = ms.main_table_mut();
        main.add_row(main.row_accessor().row(0).expect("first row").clone())
            .expect("repeat that baseline");
        let other = main
            .cell_accessor(1, "DATA")
            .and_then(|cell| cell.array())
            .expect("another visibility")
            .clone();
        main.row_accessor_mut()
            .set_cell(extra, "DATA", Value::Array(other))
            .expect("distinct data");
        for row in [1, extra] {
            set_key(main, row, column, new_value);
        }
        ms.save().expect("save");
        drop(ms);
        let observation =
            Msv2Observation::open(&path).unwrap_or_else(|error| panic!("{column}: {error}"));
        let partitions = observation.catalogue().partition_ids();
        assert_eq!(partitions.len(), 2, "{column}");
        if column == "PROCESSOR_ID" {
            for (id, entry) in observation.catalogue().partitions.iter().enumerate() {
                assert_eq!(entry.processor, Some(ProcessorId(id as u32)));
            }
        }
        let ms = MeasurementSet::open(&path).expect("read physical rows");
        check_traversal(&observation, &ms);
    }
}

fn int_key(main: &Table, row: usize, column: &str) -> i32 {
    let ScalarValue::Int32(value) = main
        .cell_accessor(row, column)
        .and_then(|cell| cell.scalar())
        .expect(column)
    else {
        panic!("{column} is an integer");
    };
    *value
}

/// Compare each occupied grid cell with physical MAIN, and account for
/// every row and every padding cell across both partitions.
fn check_traversal(observation: &Msv2Observation, ms: &MeasurementSet) {
    let catalogue = observation.catalogue();
    let access = AccessRequest {
        partitions: catalogue.partition_ids(),
        ..request(0..1, usize::MAX)
    };
    let mut read = observation.bind_read(&access).expect("bind");
    let data = ms.data_column(VisibilityDataColumn::Data).expect("DATA");
    let main = ms.main_table();
    let mjd_zero = MjdHighPrec::new(0.0, 0.0);
    let mut seen = vec![false; ms.row_count()];
    let mut padding = 0;
    let mut buffers = BatchBuffers::default();
    while let Some(event) = read.next(&mut buffers).expect("read") {
        let ReadEvent::Batch(batch) = event else {
            continue;
        };
        let values = batch.group.values.expect("values");
        let flags = batch.group.flags.expect("flags");
        for i in 0..batch.samples() {
            let sample = batch.sample(i);
            let time = batch
                .time
                .seconds_since(i / batch.samples_per_time(), mjd_zero);
            let Some(row) = observation.main_row(batch.partition, time, sample.baseline) else {
                padding += 1;
                assert!(flags.slice(ndarray::s![i, .., ..]).iter().all(|&flag| flag));
                continue;
            };
            let row = row as usize;
            assert!(
                !std::mem::replace(&mut seen[row], true),
                "repeated row {row}"
            );
            assert_eq!(sample.field.0 as i32, int_key(main, row, "FIELD_ID"));
            let scan = batch.scan(i).expect("scan");
            assert_eq!(
                catalogue.scans[scan.0 as usize].number,
                Some(int_key(main, row, "SCAN_NUMBER"))
            );
            if let Some(processor) = catalogue.partitions[batch.partition.0 as usize].processor {
                assert_eq!(processor.0 as i32, int_key(main, row, "PROCESSOR_ID"));
            }
            let ArrayValue::Complex32(stored) = data.get(row).expect("row DATA") else {
                panic!("complex DATA");
            };
            for corr in 0..batch.correlations.len() {
                assert_eq!(
                    values[[i, 0, corr]],
                    stored[[corr, 0]],
                    "row {row}, correlation {corr}"
                );
            }
        }
    }
    assert!(
        seen.into_iter().all(|seen| seen),
        "some MAIN rows were lost"
    );
    let grids: usize = read
        .estimate
        .partitions
        .iter()
        .map(|p| p.times * p.baselines)
        .sum();
    let estimated_padding: f64 = read
        .estimate
        .partitions
        .iter()
        .map(|p| (p.times * p.baselines) as f64 * p.padded_fraction)
        .sum();
    assert_eq!(grids, ms.row_count() + padding);
    assert!((estimated_padding - padding as f64).abs() < 1e-9);
    assert!(padding > 0, "the affected grid has a hole");
}

fn add_weight_spectrum(main: &mut Table) {
    let schema = casa_ms::build_table_schema(&[*OptionalMainColumn::WeightSpectrum.column_def()])
        .expect("weight schema");
    main.add_column(
        schema.column("WEIGHT_SPECTRUM").expect("column").clone(),
        None,
    )
    .expect("add spectral weights");
}

/// Automatic splits keep the original weight probe and leave a second,
/// regular partition in the same store on its original grid.
#[test]
fn only_the_conflicting_partition_splits_with_one_weight_extent() {
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ms");
    split_sessions(&path, false);
    three_scans(&path);
    let regular = {
        let observation = Msv2Observation::open(&path).expect("two regular partitions");
        assert_eq!(observation.catalogue().partitions.len(), 2);
        observation
            .grid(PartitionId(1), "base")
            .expect("later session")
    };

    let mut ms = MeasurementSet::open(&path).expect("fixture");
    let main = ms.main_table_mut();
    set_key(main, 1, "SCAN_NUMBER", 4);
    add_weight_spectrum(main);
    main.row_accessor_mut()
        .set_cell(
            1,
            "WEIGHT_SPECTRUM",
            Value::Array(ArrayValue::Float32(ndarray::ArrayD::from_elem(
                vec![4, 8],
                3.0,
            ))),
        )
        .expect("only the new scan's first row has spectral weights");
    ms.save().expect("save");
    drop(ms);

    let observation = Msv2Observation::open(&path).expect("adaptive grids");
    let catalogue = observation.catalogue();
    assert_eq!(
        catalogue
            .partitions
            .iter()
            .map(|partition| partition.observation.0)
            .collect::<Vec<_>>(),
        [0, 0, 0, 1],
    );
    let later = observation
        .grid(PartitionId(3), "base")
        .expect("later session");
    assert_eq!(regular.times_s, later.times_s);
    assert_eq!(regular.baselines, later.baselines);
    assert_eq!(regular.scan_per_time, later.scan_per_time);
    let access = AccessRequest {
        partitions: catalogue.partition_ids(),
        ..request(0..8, usize::MAX)
    };
    let estimate = observation.estimate(&access).expect("estimate");
    assert!(
        estimate
            .partitions
            .iter()
            .all(|p| p.weights.as_ref().expect("weights").variable == "WEIGHT")
    );
}

/// Both rows have the same TIME. Processor 0 sorts first even though its
/// row is physically second. Only that row has spectral weights.
#[test]
fn processor_weight_probes_follow_the_requested_extent() {
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ms");
    let mut ms = MeasurementSet::open(&path).expect("fixture");
    add_processors(&mut ms);
    let main = ms.main_table_mut();
    main.remove_rows(&(2..main.row_count()).collect::<Vec<_>>())
        .expect("keep the first two acquisitions");
    set_key(main, 0, "PROCESSOR_ID", 1);
    let scalar = [[2.0, 3.0, 4.0, 5.0], [6.0, 7.0, 8.0, 9.0]];
    for (row, weights) in scalar.iter().enumerate() {
        main.row_accessor_mut()
            .set_cell(
                row,
                "WEIGHT",
                Value::Array(ArrayValue::Float32(
                    ndarray::Array1::from_vec(weights.to_vec()).into_dyn(),
                )),
            )
            .expect("scalar weights");
    }
    add_weight_spectrum(main);
    let spectral = ndarray::ArrayD::from_shape_fn(vec![4, 8], |i| 50.0 + (10 * i[1] + i[0]) as f32);
    main.row_accessor_mut()
        .set_cell(
            1,
            "WEIGHT_SPECTRUM",
            Value::Array(ArrayValue::Float32(spectral)),
        )
        .expect("spectral weights for the physically second row");
    ms.save().expect("save");
    drop(ms);

    let default = Msv2Observation::open(&path).expect("open with default weight scope");
    let default_weights = processor_weights(&default);
    assert_eq!(
        default_weights,
        [
            [50.0, 51.0, 52.0, 53.0, 60.0, 61.0, 62.0, 63.0],
            [2.0, 3.0, 4.0, 5.0, 2.0, 3.0, 4.0, 5.0],
        ]
    );
    drop(default);

    let imaging = Msv2Observation::open_with(
        &path,
        &OpenOptions {
            partition_scheme: PartitionScheme {
                fields: true,
                ..PartitionScheme::default()
            },
            weight_spectrum_unsplit: true,
            visibility_frequency_frame: true,
            ..OpenOptions::default()
        },
    )
    .expect("open with imaging weight scope");
    let imaging_weights = processor_weights(&imaging);
    assert_eq!(
        imaging_weights,
        [
            [6.0, 7.0, 8.0, 9.0, 6.0, 7.0, 8.0, 9.0],
            [2.0, 3.0, 4.0, 5.0, 2.0, 3.0, 4.0, 5.0],
        ]
    );
    eprintln!("processor order [0, 1]: default {default_weights:?}, imaging {imaging_weights:?}");
}

fn processor_weights(observation: &Msv2Observation) -> Vec<Vec<f32>> {
    let catalogue = observation.catalogue();
    let ids = catalogue.partition_ids();
    assert_eq!(ids, [PartitionId(0), PartitionId(1)]);
    assert_eq!(observation.first_row(ids[0]), Some(1));
    assert_eq!(observation.first_row(ids[1]), Some(0));
    let times = |id| observation.grid(id, "base").expect("grid").times_s;
    assert_eq!(times(ids[0]), times(ids[1]), "the probes tie on TIME");
    let access = AccessRequest {
        partitions: ids,
        ..request(0..2, usize::MAX)
    };
    let mut traversal = observation.bind_read(&access).expect("bind");
    let mut buffers = BatchBuffers::default();
    let mut delivered = Vec::new();
    while let Some(event) = traversal.next(&mut buffers).expect("read the weights") {
        if let ReadEvent::Batch(batch) = event {
            assert_eq!(
                catalogue.partitions[batch.partition.0 as usize].processor,
                Some(ProcessorId(delivered.len() as u32))
            );
            assert_eq!(batch.shape(), [1, 2, 4]);
            delivered.push(
                batch
                    .group
                    .weights
                    .expect("weights")
                    .iter()
                    .copied()
                    .collect(),
            );
        }
    }
    delivered
}
