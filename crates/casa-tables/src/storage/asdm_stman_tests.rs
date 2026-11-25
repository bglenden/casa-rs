// SPDX-License-Identifier: LGPL-3.0-or-later
//! Checked cells and supported write behavior for a synthetic ASDM binding.
use super::*;
use crate::storage::table_control::{
    ColumnDescContents, PlainColumnEntry, read_table_dat, write_table_dat,
};
use crate::{ColumnSchema, Table, TableOptions, TableSchema};
use casa_types::{RecordField, RecordValue, ScalarValue, Value};

thread_local! {
    pub(super) static INDEX_OPENS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(super) static BDF_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[derive(Clone, Copy)]
struct Block {
    format: i16,
    dimensions: [u32; 6],
    row: i64,
    offset: i64,
    window_offset: u32,
    scales: &'static [f64],
}

fn index(path: &Path, bdf: &Path, big_endian: bool, blocks: &[Block]) {
    let mut io = AipsIo::open(path, AipsOpenOption::New).unwrap();
    io.putstart(TYPE, 1).unwrap();
    io.put_bool(big_endian).unwrap();
    io.putstart("Block", 1).unwrap();
    io.put_u32(1).unwrap();
    io.put_string(bdf.to_str().unwrap()).unwrap();
    io.putend().unwrap();
    io.put_u32(blocks.len() as u32).unwrap();
    for block in blocks {
        io.put_i16(1).unwrap();
        io.put_i16(block.format).unwrap();
        io.put_u32(0).unwrap();
        io.put_u32_slice(&block.dimensions, false).unwrap();
        io.put_i64(block.row).unwrap();
        io.put_i64(block.offset).unwrap();
        io.put_u32(block.window_offset).unwrap();
        io.put_f64_slice(block.scales, true).unwrap();
    }
    io.putend().unwrap();
    io.close().unwrap();
}

fn binding(directory: &Path, bdf: &Path, big_endian: bool, blocks: &[Block], rows: usize) {
    let mut table = Table::with_schema(
        TableSchema::new(vec![ColumnSchema::scalar("ID", PrimitiveType::Int32)]).unwrap(),
    );
    for id in 0..rows {
        let row = RecordValue::new(vec![RecordField::new(
            "ID",
            Value::Scalar(ScalarValue::Int32(id as i32)),
        )]);
        table.add_row(row).unwrap();
    }
    table.save(TableOptions::new(directory)).unwrap();
    let control = directory.join("table.dat");
    let mut table = read_table_dat(&control).unwrap();
    table.column_set.seq_count = 2;
    table.column_set.data_managers.push(DataManagerEntry {
        seq_nr: 1,
        type_name: TYPE.into(),
        data: Vec::new(),
    });
    for (name, dtype, dims) in [
        ("DATA", PrimitiveType::Complex32, 2),
        ("FLAG", PrimitiveType::Bool, 2),
        ("SIGMA", PrimitiveType::Float32, 1),
        ("WEIGHT", PrimitiveType::Float32, 1),
    ] {
        let schema = ColumnSchema::array_variable(name, dtype, Some(dims));
        let mut desc = ColumnDescContents::from_column_schema(&schema);
        desc.data_manager_type = TYPE.into();
        desc.data_manager_group = TYPE.into();
        table.table_desc.columns.push(desc);
        table.column_set.columns.push(PlainColumnEntry {
            original_name: name.into(),
            dm_seq_nr: 1,
            is_array: true,
        });
    }
    index(
        &directory.join("table.f1asdmindex"),
        bdf,
        big_endian,
        blocks,
    );
    write_table_dat(&control, &table).unwrap();
}

fn encoded_cross_shorts(big_endian: bool) -> Vec<u8> {
    // On disk, six baselines of four antennas in triangular BDF order. Each
    // baseline contains one unrelated value followed by this SPW's two
    // channels with three complex correlations.
    let mut bytes = vec![0x5a; 16];
    for baseline in 0..6_i16 {
        let packed = std::iter::once((200, 200)).chain((0..6_i16).map(|lane| {
            let real = baseline * 30 + lane + 2;
            (real, -real)
        }));
        for (real, imag) in packed {
            for value in [real, imag] {
                bytes.extend(if big_endian {
                    value.to_be_bytes()
                } else {
                    value.to_le_bytes()
                });
            }
        }
    }
    bytes
}

const CROSS: Block = Block {
    format: 0,
    dimensions: [6, 2, 2, 3, 7, 1],
    row: 0,
    offset: 16,
    window_offset: 1,
    scales: &[1., 2.],
};

fn data_values(cell: &ArrayValue) -> &[Complex32] {
    let ArrayValue::Complex32(values) = cell else {
        panic!("expected complex data");
    };
    values.as_slice_memory_order().unwrap()
}

#[test]
fn retargeting_a_saved_view_preserves_cross_endian_visibility_cells() {
    use crate::TableWriteLock;

    for big_endian in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let initial = root.path().join("original-asdm");
        let bdf = initial.join("ASDMBinary/samples.bdf");
        std::fs::create_dir_all(bdf.parent().unwrap()).unwrap();
        std::fs::write(&bdf, encoded_cross_shorts(big_endian)).unwrap();
        let source = root.path().join("lazy");
        binding(&source, &bdf, big_endian, &[CROSS], 6);
        let control = source.join("table.dat");
        let mut descriptor = read_table_dat(&control).unwrap();
        descriptor
            .table_desc
            .columns
            .iter_mut()
            .find(|column| column.col_name == "DATA")
            .unwrap()
            .data_manager_group = "DeclaredDataGroup".into();
        write_table_dat(&control, &descriptor).unwrap();
        let read = |table: &Table| {
            table
                .column_accessor("DATA")
                .unwrap()
                .array_cells_owned_uncached(&[0, 5])
                .unwrap()
        };
        let table = Table::open(TableOptions::new(&source)).unwrap();
        let expected = read(&table);
        let view = root.path().join("view");
        table
            .select_rows(&[5, 0, 2])
            .unwrap()
            .save(TableOptions::new(&view))
            .unwrap();
        let saved = Table::open(TableOptions::new(&view)).unwrap();
        let index = saved.asdm_bdf_index().unwrap().unwrap();
        assert_eq!(index.reference().unwrap(), initial.to_str().unwrap());
        let relocated = root.path().join("relocated-asdm");
        std::fs::rename(initial, &relocated).unwrap();

        let lock = TableWriteLock::acquire(&source, 1).unwrap();
        let spelling = format!("{}/{}", relocated.display(), "/");
        assert_eq!(index.relocate(&spelling, &lock).unwrap(), spelling);
        lock.release().unwrap();
        let reference = format!("{}/", relocated.display());
        assert_eq!(index.reference().unwrap(), reference);
        let reopened = Table::open(TableOptions::new(&source)).unwrap();
        let current = reopened.asdm_bdf_index().unwrap().unwrap();
        assert_eq!(current.reference().unwrap(), reference);
        assert_eq!(read(&reopened), expected);
    }
}

#[test]
fn crossed_baselines_are_reordered_and_materialize_independently() {
    for big_endian in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let bdf = root.path().join("samples.bdf");
        std::fs::write(&bdf, encoded_cross_shorts(big_endian)).unwrap();
        let source = root.path().join("lazy");
        binding(&source, &bdf, big_endian, &[CROSS], 6);
        let mut table = Table::open(TableOptions::new(&source)).unwrap();

        let rows = [5, 2, 0, 2, 3, 1, 4];
        BDF_READS.with(|count| count.set(0));
        let cells = table
            .column_accessor("DATA")
            .unwrap()
            .array_cells_owned_uncached(&rows)
            .unwrap();
        assert_eq!(BDF_READS.with(std::cell::Cell::get), 1);
        let physical_rows = [0, 1, 3, 2, 4, 5];
        for (&row, cell) in rows.iter().zip(cells) {
            let cell = cell.unwrap();
            assert_eq!(cell.shape(), &[3, 2]);
            let expected = (0..6)
                .map(|lane| {
                    let value = (physical_rows[row] * 30 + lane + 2) as f32 / 2.;
                    Complex32::new(value, -value)
                })
                .collect::<Vec<_>>();
            assert_eq!(data_values(&cell), expected);
        }
        let original = table
            .cell_accessor(0, "DATA")
            .unwrap()
            .array()
            .unwrap()
            .clone();
        assert!(table.cell_accessor_mut(0, "DATA").is_err());
        assert!(table.column_accessor_mut("DATA").is_err());
        table
            .row_accessor_mut()
            .set_cell(0, "ID", Value::Scalar(ScalarValue::Int32(99)))
            .unwrap();
        let mixed = table
            .prepare_write()
            .save_selected_rows(&["ID", "DATA"], &[0]);
        assert!(mixed.is_err());
        assert!(table.flush().is_err());

        let reopened = Table::open(TableOptions::new(&source)).unwrap();
        assert_eq!(
            reopened.cell_accessor(0, "ID").unwrap().scalar().unwrap(),
            &ScalarValue::Int32(0)
        );
        assert_eq!(
            reopened.cell_accessor(0, "DATA").unwrap().array().unwrap(),
            &original
        );
        table
            .prepare_write()
            .save_selected_rows(&["ID"], &[0])
            .unwrap();
        let reread = Table::open(TableOptions::new(&source)).unwrap();
        assert_eq!(
            reread.cell_accessor(0, "ID").unwrap().scalar().unwrap(),
            &ScalarValue::Int32(99)
        );

        let output = root.path().join("eager");
        table.save(TableOptions::new(&output)).unwrap();
        std::fs::remove_file(&bdf).unwrap();
        let independent = Table::open(TableOptions::new(output)).unwrap();
        assert_eq!(
            independent
                .cell_accessor(0, "DATA")
                .unwrap()
                .array()
                .unwrap(),
            &original
        );
        let metadata_only = Table::open(TableOptions::new(&source)).unwrap();
        assert_eq!(
            metadata_only.array_shape_uncached(0, "DATA").unwrap(),
            Some(vec![3, 2])
        );
        assert!(metadata_only.is_cell_defined(0, "DATA").unwrap());
        assert!(
            metadata_only
                .cell_accessor(0, "DATA")
                .unwrap()
                .array()
                .is_err()
        );
        let flag = metadata_only
            .cell_accessor(0, "FLAG")
            .unwrap()
            .array()
            .unwrap();
        assert_eq!(
            flag,
            &ArrayValue::Bool(ArrayD::from_elem(IxDyn(&[3, 2]), false))
        );
        for name in ["SIGMA", "WEIGHT"] {
            let weight = metadata_only
                .cell_accessor(0, name)
                .unwrap()
                .array()
                .unwrap();
            assert_eq!(
                weight,
                &ArrayValue::Float32(ArrayD::from_elem(IxDyn(&[3]), 1.))
            );
        }
    }
}

#[test]
fn packed_autocorrelations_and_other_cross_encodings_follow_descriptor_transitions() {
    let root = tempfile::tempdir().unwrap();
    let bdf = root.path().join("encodings.bdf");
    let index_file = root.path().join("index");
    let float_samples = [
        1.0_f32, 2., 3., 4., 5., 6., 7., 8., 9., 10., 11., 12., 13., 14., 15., 16.,
    ];
    let mut bytes = float_samples
        .iter()
        .flat_map(|value| value.to_be_bytes())
        .collect::<Vec<_>>();
    bytes.extend((-840_i32).to_be_bytes());
    bytes.extend(420_i32.to_be_bytes());
    let block = |format, polarizations, row, offset, step| Block {
        format,
        dimensions: [1, 1, 1, polarizations, step, 0],
        row,
        offset,
        window_offset: 0,
        scales: &[2.],
    };
    let descriptors = [
        block(10, 3, 0, 0, 4),
        block(10, 4, 1, 16, 6),
        block(10, 2, 2, 40, 2),
        block(3, 1, 3, 48, 1),
        block(1, 1, 4, 64, 1),
    ];
    std::fs::write(&bdf, bytes).unwrap();
    index(&index_file, &bdf, true, &descriptors);
    let source = Index::open(&index_file, 5).unwrap();
    let actual = source.read(Column::Data, [4, 1, 0, 3]).unwrap();
    let values = actual
        .iter()
        .map(|cell| data_values(cell.as_ref().unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(values[0], &[Complex32::new(-420., 210.)]);
    assert_eq!(
        values[1],
        &[
            Complex32::new(5., 0.),
            Complex32::new(6., 7.),
            Complex32::new(8., 9.),
            Complex32::new(10., 0.),
        ]
    );
    assert_eq!(
        values[2],
        &[
            Complex32::new(1., 0.),
            Complex32::new(2., 3.),
            Complex32::new(2., -3.),
            Complex32::new(4., 0.),
        ]
    );
    assert_eq!(values[3], &[Complex32::new(6.5, 7.)]);
    let float = source.read(Column::FloatData, [2]).unwrap();
    assert_eq!(
        float[0],
        Some(ArrayValue::Float32(
            ArrayD::from_shape_vec(IxDyn(&[2, 1]), vec![11., 12.]).unwrap()
        )),
    );
    assert!(source.read(Column::FloatData, [0]).is_err());
    assert!(source.read(Column::FloatData, [3]).is_err());
    assert!(source.read(Column::Data, [5]).is_err());
}

#[test]
fn malformed_index_and_short_payload_fail_before_decoding() {
    let root = tempfile::tempdir().unwrap();
    let bdf = root.path().join("truncated.bdf");
    let file = root.path().join("index");
    std::fs::write(&bdf, [0; 32]).unwrap();
    index(&file, &bdf, true, &[CROSS]);
    let source = Index::open(&file, 6).unwrap();
    assert_eq!(
        source.shapes(Column::Data, &[5]).unwrap(),
        vec![Some(vec![3, 2])]
    );
    assert!(source.read(Column::Data, [0]).is_err());
    for bad in [
        Block {
            offset: -1,
            ..CROSS
        },
        Block { row: -1, ..CROSS },
        Block {
            window_offset: 3,
            ..CROSS
        },
        Block {
            scales: &[1., 0.],
            ..CROSS
        },
    ] {
        index(&file, &bdf, true, &[bad]);
        assert!(Index::open(&file, 6).is_err());
    }
}

#[test]
fn selected_prepared_and_full_reads_share_one_index() {
    let root = tempfile::tempdir().unwrap();
    let bdf = root.path().join("samples.bdf");
    std::fs::write(&bdf, encoded_cross_shorts(true)).unwrap();
    let source = root.path().join("lazy");
    binding(&source, &bdf, true, &[CROSS], 6);
    INDEX_OPENS.with(|count| count.set(0));

    let table = Table::open(TableOptions::new(&source)).unwrap();
    let data = table.column_accessor("DATA").unwrap();
    assert_eq!(
        data.array_cells_owned_uncached(&[2, 0]).unwrap()[0]
            .as_ref()
            .unwrap()
            .shape(),
        &[3, 2],
    );
    assert_eq!(
        table.column_accessor("ID").unwrap().scalar_cell(1).unwrap(),
        &ScalarValue::Int32(1),
    );
    let mut prepared = table.row_accessor().prepare(&["DATA", "ID"]).unwrap();
    prepared.load(2).unwrap();
    assert_eq!(table.rows().unwrap().len(), 6);
    assert_eq!(INDEX_OPENS.with(std::cell::Cell::get), 1);
}

#[test]
fn same_path_copy_and_flush_after_column_removal_leave_the_source_unchanged() {
    let root = tempfile::tempdir().unwrap();
    let bdf = root.path().join("samples.bdf");
    std::fs::write(&bdf, encoded_cross_shorts(true)).unwrap();
    let source = root.path().join("lazy");
    binding(&source, &bdf, true, &[CROSS], 6);
    let snapshot = || {
        ["table.dat", "table.f0", "table.f1asdmindex"]
            .map(|name| std::fs::read(source.join(name)).unwrap())
    };
    let before = snapshot();
    let mut table = Table::open(TableOptions::new(&source)).unwrap();
    assert!(table.shallow_copy(TableOptions::new(&source)).is_err());
    assert_eq!(snapshot(), before);
    for name in ["DATA", "FLAG", "SIGMA", "WEIGHT"] {
        table.remove_column(name).unwrap();
    }
    assert!(
        table
            .data_manager_info()
            .iter()
            .all(|manager| manager.dm_type != TYPE)
    );
    assert!(table.flush().is_err());
    assert_eq!(snapshot(), before);
}

#[cfg(unix)]
#[test]
fn renamed_reference_rejects_asdm_mutation_before_any_group_is_persisted() {
    use crate::storage::{CompositeStorage, RefTableDatContents, TableInfo};
    use crate::{LockMode, LockOptions, LockType};

    let root = tempfile::tempdir().unwrap();
    let bdf = root.path().join("samples.bdf");
    std::fs::write(&bdf, encoded_cross_shorts(true)).unwrap();
    let source = root.path().join("lazy");
    let reference = root.path().join("view");
    binding(&source, &bdf, true, &[CROSS], 6);
    CompositeStorage
        .save_ref_table(
            &reference,
            &RefTableDatContents {
                nrrow: 2,
                big_endian: true,
                parent_relative_path: source.to_str().unwrap().into(),
                column_name_map: vec![("VISIBILITY".into(), "DATA".into())],
                column_names: vec!["ID".into(), "VISIBILITY".into()],
                parent_nrrow: 6,
                row_order: true,
                row_map: vec![0, 1],
            },
            &TableInfo::default(),
        )
        .unwrap();
    let mut view = Table::open_with_lock(
        TableOptions::new(reference),
        LockOptions::new(LockMode::UserLocking),
    )
    .unwrap();
    assert!(view.lock(LockType::Write, 1).unwrap());
    view.row_accessor_mut()
        .set_cell(0, "ID", Value::Scalar(ScalarValue::Int32(99)))
        .unwrap();
    assert!(view.cell_accessor_mut(1, "VISIBILITY").is_err());
    assert!(
        view.prepare_write()
            .save_selected_rows(&["ID", "VISIBILITY"], &[0, 1])
            .is_err()
    );
    let reopened = Table::open(TableOptions::new(source)).unwrap();
    assert_eq!(
        reopened.cell_accessor(0, "ID").unwrap().scalar().unwrap(),
        &ScalarValue::Int32(0)
    );
    let data = reopened.cell_accessor(1, "DATA").unwrap().array().unwrap();
    assert_eq!(data_values(data)[0], Complex32::new(16., -16.));
}
