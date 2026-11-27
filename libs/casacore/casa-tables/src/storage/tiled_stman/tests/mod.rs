// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use casa_core::{ArrayValue, PrimitiveType, RecordField, RecordValue, Value};
use ndarray::ArrayD;
use std::sync::{Mutex, OnceLock};
use tempfile::tempdir;

use crate::{ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema};

fn make_bool_tile_pattern(base: usize) -> Vec<bool> {
    (0..4).map(|i| (base + i).is_multiple_of(3)).collect()
}

fn shared_table_cache_test_guard() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .expect("shared table cache test lock")
}

#[test]
fn tile_element_sizes() {
    assert_eq!(tile_element_size(CasacoreDataType::TpBool), 1);
    assert_eq!(tile_element_size(CasacoreDataType::TpUChar), 1);
    assert_eq!(tile_element_size(CasacoreDataType::TpChar), 1);
    assert_eq!(tile_element_size(CasacoreDataType::TpShort), 2);
    assert_eq!(tile_element_size(CasacoreDataType::TpUShort), 2);
    assert_eq!(tile_element_size(CasacoreDataType::TpInt), 4);
    assert_eq!(tile_element_size(CasacoreDataType::TpUInt), 4);
    assert_eq!(tile_element_size(CasacoreDataType::TpFloat), 4);
    assert_eq!(tile_element_size(CasacoreDataType::TpDouble), 8);
    assert_eq!(tile_element_size(CasacoreDataType::TpInt64), 8);
    assert_eq!(tile_element_size(CasacoreDataType::TpComplex), 8);
    assert_eq!(tile_element_size(CasacoreDataType::TpDComplex), 16);
    // Unsupported types return 0.
    assert_eq!(tile_element_size(CasacoreDataType::TpString), 0);
    assert_eq!(tile_element_size(CasacoreDataType::TpTable), 0);
}

#[test]
fn bool_tile_storage_is_bit_packed() {
    assert_eq!(tile_storage_bytes(CasacoreDataType::TpBool, 4), 1);
    assert_eq!(tile_storage_bytes(CasacoreDataType::TpBool, 9), 2);
}

#[test]
fn complex32_tile_row_encoder_uses_fortran_cell_order() {
    let values = vec![
        Complex32::new(0.0, -0.0),
        Complex32::new(1.0, -1.0),
        Complex32::new(2.0, -2.0),
        Complex32::new(10.0, -10.0),
        Complex32::new(11.0, -11.0),
        Complex32::new(12.0, -12.0),
    ];
    let array = ArrayD::from_shape_vec(vec![2, 3], values).expect("array");
    let mut encoded = vec![0u8; 2 * 3 * 8];

    encode_complex32_array_into_tile_row(&mut encoded, &array, &[2, 3], false).expect("encode row");

    let decoded = encoded
        .as_chunks::<8>()
        .0
        .iter()
        .map(|chunk| {
            let re = f32::from_le_bytes(chunk[0..4].try_into().unwrap());
            let im = f32::from_le_bytes(chunk[4..8].try_into().unwrap());
            Complex32::new(re, im)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        decoded,
        vec![
            Complex32::new(0.0, -0.0),
            Complex32::new(10.0, -10.0),
            Complex32::new(1.0, -1.0),
            Complex32::new(11.0, -11.0),
            Complex32::new(2.0, -2.0),
            Complex32::new(12.0, -12.0),
        ]
    );
}

#[test]
fn streamed_complex32_tiled_shape_writer_installs_readable_column() {
    let dir = tempdir().expect("tempdir");
    let table_path = dir.path().join("streamed.table");
    let temp_data_path = dir.path().join("DATA.tmp");
    let row_count = 5usize;
    let cell_shape = vec![2, 3];
    let tile_shape = vec![2, 2, 2];
    let mut writer = StreamingTiledShapeComplex32Writer::create(
        &temp_data_path,
        row_count,
        cell_shape.clone(),
        tile_shape.clone(),
        STREAMING_TILED_COLUMN_BUFFER_BYTES,
        false,
    )
    .expect("create streaming writer");

    for row in 0..row_count {
        let mut values = Vec::with_capacity(cell_shape.iter().product());
        for channel in 0..cell_shape[1] {
            for corr in 0..cell_shape[0] {
                let value = row as f32 * 100.0 + channel as f32 * 10.0 + corr as f32;
                values.push(Complex32::new(value, -value));
            }
        }
        writer.push_row(&values).expect("push streamed row");
    }

    let streamed = writer.finish().expect("finish writer");
    let expected_bytes = 6 * 2 * 2 * 2 * std::mem::size_of::<Complex32>();
    assert_eq!(streamed.row_count(), row_count);
    assert_eq!(streamed.bytes_written(), expected_bytes);
    install_streamed_tiled_shape_complex32_column(&table_path, 0, "DATA", streamed)
        .expect("install streamed column");

    let (variant, header) =
        read_tiled_header(&table_path.join("table.f0")).expect("read streamed header");
    let TiledVariant::Shape {
        default_tile_shape,
        nr_used_row_map,
        row_map,
        cube_map,
        pos_map,
    } = variant
    else {
        panic!("expected TiledShapeStMan header");
    };
    assert_eq!(default_tile_shape, vec![2, 2, 2]);
    assert_eq!(nr_used_row_map, 1);
    assert_eq!(row_map, vec![(row_count - 1) as u32]);
    assert_eq!(cube_map, vec![1]);
    assert_eq!(pos_map, vec![(row_count - 1) as u32]);
    assert_eq!(header.nrrow, row_count as u64);
    assert_eq!(
        header.files[0].as_ref().expect("file").length,
        expected_bytes as i64
    );
    assert_eq!(header.cubes[1].cube_shape, vec![2, 3, row_count]);
    assert_eq!(header.cubes[1].tile_shape, tile_shape);

    let file_data = std::fs::read(tsm_data_path(&table_path, 0, 0)).expect("read TSM file");
    let (bucket_size, offsets) =
        compute_tile_layout(&header.col_data_types, &header.cubes[1].tile_shape);
    let raw = reconstruct_cube_column(
        &file_data,
        header.cubes[1].file_offset as usize,
        &header.cubes[1].cube_shape,
        &header.cubes[1].tile_shape,
        offsets[0],
        bucket_size,
        CasacoreDataType::TpComplex,
        std::mem::size_of::<Complex32>(),
    )
    .expect("reconstruct streamed cube");

    for row in 0..row_count {
        for channel in 0..cell_shape[1] {
            for corr in 0..cell_shape[0] {
                let idx = corr + channel * cell_shape[0] + row * cell_shape[0] * cell_shape[1];
                let bytes = &raw[idx * 8..idx * 8 + 8];
                let re = f32::from_le_bytes(bytes[0..4].try_into().unwrap());
                let im = f32::from_le_bytes(bytes[4..8].try_into().unwrap());
                let expected = row as f32 * 100.0 + channel as f32 * 10.0 + corr as f32;
                assert_eq!(Complex32::new(re, im), Complex32::new(expected, -expected));
            }
        }
    }
}

#[test]
fn streamed_heterogeneous_tiled_shape_writer_uses_one_cube_per_shape() {
    let dir = tempdir().expect("tempdir");
    let table_path = dir.path().join("heterogeneous.table");
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "DATA",
        PrimitiveType::Complex32,
        Some(2),
    )])
    .expect("schema");
    let mut descriptor = Table::with_schema(schema);
    for shape in [[2, 3], [2, 2], [2, 3], [2, 2], [2, 3]] {
        descriptor
            .add_row(RecordValue::new(vec![RecordField::new(
                "DATA",
                Value::Array(ArrayValue::Complex32(
                    ArrayD::from_shape_vec(
                        IxDyn(&shape).f(),
                        vec![Complex32::new(0.0, 0.0); shape.iter().product()],
                    )
                    .expect("descriptor cell"),
                )),
            )]))
            .expect("descriptor row");
    }
    descriptor
        .save(TableOptions::new(&table_path).with_data_manager(DataManagerKind::TiledShapeStMan))
        .expect("save descriptor table");
    let layouts = vec![
        StreamedTiledShapeCubeLayout::new(vec![2, 3], 3, vec![2, 2, 2]),
        StreamedTiledShapeCubeLayout::new(vec![2, 2], 2, vec![2, 2, 3]),
    ];
    let mut writer = StreamingTiledShapeWriter::create(
        dir.path().join("DATA.tmp"),
        StreamedTiledShapeValueType::Complex32,
        layouts,
        4096,
        false,
    )
    .expect("create heterogeneous writer");

    let rows = [
        (vec![2, 3], 10.0f32),
        (vec![2, 2], 20.0),
        (vec![2, 3], 30.0),
        (vec![2, 2], 40.0),
        (vec![2, 3], 50.0),
    ];
    for (shape, base) in &rows {
        let values = (0..shape.iter().product::<usize>())
            .map(|offset| Complex32::new(base + offset as f32, -(base + offset as f32)))
            .collect::<Vec<_>>();
        writer
            .push_complex32_row(shape, &values)
            .expect("push heterogeneous row");
    }

    let streamed = writer.finish().expect("finish heterogeneous writer");
    assert_eq!(streamed.row_count(), rows.len());
    assert_eq!(streamed.shape_count(), 2);
    assert!(streamed.max_resident_bytes() <= 4096);
    install_streamed_tiled_shape_column(&table_path, 0, "DATA", streamed)
        .expect("install heterogeneous column");

    let (variant, header) = read_tiled_header(&table_path.join("table.f0")).expect("read header");
    let TiledVariant::Shape {
        nr_used_row_map,
        row_map,
        cube_map,
        pos_map,
        ..
    } = variant
    else {
        panic!("expected TiledShapeStMan header");
    };
    assert_eq!(header.nrrow, 5);
    assert_eq!(header.cubes.len(), 3);
    assert_eq!(header.cubes[1].cube_shape, vec![2, 3, 3]);
    assert_eq!(header.cubes[2].cube_shape, vec![2, 2, 2]);
    assert_eq!(nr_used_row_map, 5);
    assert_eq!(row_map, vec![0, 1, 2, 3, 4]);
    assert_eq!(cube_map, vec![1, 2, 1, 2, 1]);
    assert_eq!(pos_map, vec![0, 0, 1, 1, 2]);

    for cube_index in 1..header.cubes.len() {
        let cube = &header.cubes[cube_index];
        let file_data = std::fs::read(tsm_data_path(&table_path, 0, cube.file_seq_nr as u32))
            .expect("read shape cube");
        let (bucket_size, offsets) = compute_tile_layout(&header.col_data_types, &cube.tile_shape);
        let raw = reconstruct_cube_column(
            &file_data,
            0,
            &cube.cube_shape,
            &cube.tile_shape,
            offsets[0],
            bucket_size,
            CasacoreDataType::TpComplex,
            8,
        )
        .expect("reconstruct shape cube");
        let shape = &cube.cube_shape[..cube.cube_shape.len() - 1];
        let bases: &[f32] = if cube_index == 1 {
            &[10.0, 30.0, 50.0]
        } else {
            &[20.0, 40.0]
        };
        let cell_nelem = shape.iter().product::<usize>();
        for (row, base) in bases.iter().copied().enumerate() {
            for offset in 0..cell_nelem {
                let start = (row * cell_nelem + offset) * 8;
                let re = f32::from_le_bytes(raw[start..start + 4].try_into().unwrap());
                let im = f32::from_le_bytes(raw[start + 4..start + 8].try_into().unwrap());
                let expected = base + offset as f32;
                assert_eq!(Complex32::new(re, im), Complex32::new(expected, -expected));
            }
        }
    }

    let reopened = Table::open(TableOptions::new(&table_path)).expect("open streamed table");
    let cells = reopened
        .get_array_cells_owned("DATA", &[0, 1, 2, 3, 4])
        .expect("read streamed cells");
    for ((cell, (shape, base)), row) in cells.iter().zip(rows.iter()).zip(0usize..) {
        let ArrayValue::Complex32(values) = cell.as_ref().expect("defined cell") else {
            panic!("row {row} should contain Complex32 DATA");
        };
        assert_eq!(values.shape(), shape);
        for channel in 0..shape[1] {
            for correlation in 0..shape[0] {
                let expected = base + (correlation + channel * shape[0]) as f32;
                assert_eq!(
                    values[[correlation, channel]],
                    Complex32::new(expected, -expected)
                );
            }
        }
    }
}

#[test]
fn shape_row_intervals_exclude_undefined_cells_before_and_between_cubes() {
    let mut locations = [(6, 2, 0), (2, 1, 1), (5, 1, 2), (1, 1, 0)];
    assert_eq!(
        shape_row_intervals(&mut locations),
        (
            vec![0, 2, 4, 5, 6],
            vec![0, 1, 0, 1, 2],
            vec![0, 1, 0, 2, 0]
        ),
    );
}

#[test]
fn streamed_heterogeneous_writer_omits_unused_cube_payloads_and_remaps_rows() {
    let dir = tempdir().expect("tempdir");
    let table_path = dir.path().join("undefined-shapes.table");
    let base = dir.path().join("OPTIONAL.tmp");
    let mut writer = StreamingTiledShapeWriter::create(
        &base,
        StreamedTiledShapeValueType::Float32,
        vec![
            StreamedTiledShapeCubeLayout::new(vec![2, 3], 3, vec![2, 3, 2]),
            StreamedTiledShapeCubeLayout::new(vec![2, 2], 1, vec![2, 2, 2]),
        ],
        4096,
        false,
    )
    .expect("create optional writer");
    writer.push_undefined_row().expect("undefined row 0");
    writer
        .push_f32_row(&[2, 2], &[1.0, 2.0, 3.0, 4.0])
        .expect("defined row 1");
    writer.push_undefined_row().expect("undefined row 2");
    writer.push_undefined_row().expect("undefined row 3");

    let streamed = writer.finish().expect("finish optional writer");
    assert_eq!(streamed.shape_count(), 1);
    assert_eq!(streamed.bytes_written(), 32);
    assert!(!streamed_shape_cube_temp_path(&base, 0).exists());
    assert!(streamed_shape_cube_temp_path(&base, 1).exists());
    install_streamed_tiled_shape_column(&table_path, 0, "OPTIONAL", streamed)
        .expect("install optional column");

    let (variant, header) =
        read_tiled_header(&table_path.join("table.f0")).expect("read optional header");
    let TiledVariant::Shape {
        row_map,
        cube_map,
        pos_map,
        ..
    } = variant
    else {
        panic!("expected TiledShapeStMan header");
    };
    assert_eq!(header.nrdim, 3);
    assert_eq!(header.cubes.len(), 2);
    assert_eq!(header.cubes[1].cube_shape, vec![2, 2, 1]);
    assert_eq!(row_map, vec![0, 1, 3]);
    assert_eq!(cube_map, vec![0, 1, 0]);
    assert_eq!(pos_map, vec![0, 0, 1]);
}

#[test]
fn streamed_heterogeneous_writer_emits_no_payload_for_all_undefined_rows() {
    let dir = tempdir().expect("tempdir");
    let table_path = dir.path().join("all-undefined.table");
    let base = dir.path().join("OPTIONAL.tmp");
    let mut writer = StreamingTiledShapeWriter::create(
        &base,
        StreamedTiledShapeValueType::Float64,
        vec![StreamedTiledShapeCubeLayout::new(
            vec![4, 8],
            4,
            vec![4, 4, 2],
        )],
        4096,
        false,
    )
    .expect("create all-undefined writer");
    for _ in 0..4 {
        writer.push_undefined_row().expect("undefined row");
    }
    let streamed = writer.finish().expect("finish all-undefined writer");
    assert_eq!(streamed.shape_count(), 0);
    assert_eq!(streamed.bytes_written(), 0);
    assert!(!streamed_shape_cube_temp_path(&base, 0).exists());
    install_streamed_tiled_shape_column(&table_path, 0, "OPTIONAL", streamed)
        .expect("install all-undefined column");

    let (variant, header) =
        read_tiled_header(&table_path.join("table.f0")).expect("read empty header");
    let TiledVariant::Shape {
        nr_used_row_map,
        row_map,
        cube_map,
        pos_map,
        ..
    } = variant
    else {
        panic!("expected TiledShapeStMan header");
    };
    assert_eq!(header.nrrow, 4);
    assert_eq!(header.nrdim, 3);
    assert_eq!(header.cubes.len(), 1);
    assert!(header.files.is_empty());
    assert_eq!(nr_used_row_map, 0);
    assert!(row_map.is_empty());
    assert!(cube_map.is_empty());
    assert!(pos_map.is_empty());

    let col_desc = ColumnDescContents {
        class_name: "ArrayColumnDesc<double>".to_string(),
        col_name: "OPTIONAL".to_string(),
        comment: String::new(),
        data_manager_type: "TiledShapeStMan".to_string(),
        data_manager_group: "OPTIONAL".to_string(),
        data_type: CasacoreDataType::TpArrayDouble,
        option: 0,
        nrdim: 2,
        shape: vec![4, 8],
        max_length: 0,
        keywords: RecordValue::default(),
        is_array: true,
        primitive_type: Some(PrimitiveType::Float64),
    };
    let selected = load_tiled_column_rows_shape_variant_2d_channel_range_typed(
        &table_path,
        0,
        &header,
        0,
        &col_desc,
        CasacoreDataType::TpDouble,
        8,
        &[0, 3],
        &ShapeRowMapping {
            nr_used_row_map,
            row_map: &row_map,
            cube_map: &cube_map,
            pos_map: &pos_map,
        },
        0,
        1,
    )
    .expect("typed selected read of all-undefined column");
    assert_eq!(selected, None);
}

#[test]
fn streamed_heterogeneous_bool_rows_preserve_cross_byte_tile_bits() {
    let dir = tempdir().expect("tempdir");
    let table_path = dir.path().join("bool-shape.table");
    let mut writer = StreamingTiledShapeWriter::create(
        dir.path().join("FLAG.tmp"),
        StreamedTiledShapeValueType::Bool,
        vec![StreamedTiledShapeCubeLayout::new(
            vec![2, 3],
            3,
            vec![2, 2, 3],
        )],
        4096,
        false,
    )
    .expect("create bool writer");
    let expected = vec![
        vec![true, false, true, false, true, false],
        vec![false, true, false, true, false, true],
        vec![true, true, false, false, true, true],
    ];
    for row in &expected {
        writer.push_bool_row(&[2, 3], row).expect("push bool row");
    }
    install_streamed_tiled_shape_column(
        &table_path,
        0,
        "FLAG",
        writer.finish().expect("finish bool writer"),
    )
    .expect("install bool column");

    let (_, header) = read_tiled_header(&table_path.join("table.f0")).expect("read header");
    let cube = &header.cubes[1];
    let file_data = std::fs::read(tsm_data_path(&table_path, 0, 0)).expect("read bool cube");
    let (bucket_size, offsets) = compute_tile_layout(&header.col_data_types, &cube.tile_shape);
    let raw = reconstruct_cube_column(
        &file_data,
        0,
        &cube.cube_shape,
        &cube.tile_shape,
        offsets[0],
        bucket_size,
        CasacoreDataType::TpBool,
        1,
    )
    .expect("reconstruct bool cube");
    let actual = raw
        .as_chunks::<6>()
        .0
        .iter()
        .map(|row| row.iter().map(|&value| value != 0).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

#[test]
fn streamed_heterogeneous_writer_rejects_an_unrealistic_memory_plan() {
    let dir = tempdir().expect("tempdir");
    let error = StreamingTiledShapeWriter::create(
        dir.path().join("DATA.tmp"),
        StreamedTiledShapeValueType::Complex32,
        vec![StreamedTiledShapeCubeLayout::new(
            vec![4, 1024],
            1,
            vec![4, 128, 1],
        )],
        1024,
        false,
    )
    .expect_err("plan should exceed memory budget");
    assert!(error.to_string().contains("exceeds memory budget"));
}

#[test]
fn streamed_primitive_writers_install_readable_columns() {
    let dir = tempdir().expect("tempdir");
    let table_path = dir.path().join("primitive.table");

    let row_count = 5usize;
    let mut flag_writer = StreamingTiledPrimitiveWriter::create_shape(
        dir.path().join("FLAG.tmp"),
        row_count,
        vec![2, 3],
        vec![2, 2, 2],
        StreamedTiledPrimitiveType::Bool,
        STREAMING_TILED_COLUMN_BUFFER_BYTES,
        false,
    )
    .expect("create FLAG writer");
    for row in 0..row_count {
        if row % 2 == 0 {
            flag_writer.push_zero_row().expect("zero FLAG row");
        } else {
            flag_writer
                .push_bool_row(&[true; 6])
                .expect("true FLAG row");
        }
    }
    let streamed_flag = flag_writer.finish().expect("finish FLAG");
    assert_eq!(streamed_flag.bytes_written(), 6);
    install_streamed_tiled_shape_primitive_column(&table_path, 0, "FLAG", streamed_flag)
        .expect("install FLAG");

    let (flag_variant, flag_header) =
        read_tiled_header(&table_path.join("table.f0")).expect("read FLAG header");
    assert!(matches!(flag_variant, TiledVariant::Shape { .. }));
    assert_eq!(flag_header.cubes[1].cube_shape, vec![2, 3, row_count]);
    let flag_file = std::fs::read(tsm_data_path(&table_path, 0, 0)).expect("read FLAG TSM");
    let (flag_bucket_size, flag_offsets) = compute_tile_layout(
        &flag_header.col_data_types,
        &flag_header.cubes[1].tile_shape,
    );
    let flag_raw = reconstruct_cube_column(
        &flag_file,
        flag_header.cubes[1].file_offset as usize,
        &flag_header.cubes[1].cube_shape,
        &flag_header.cubes[1].tile_shape,
        flag_offsets[0],
        flag_bucket_size,
        CasacoreDataType::TpBool,
        1,
    )
    .expect("reconstruct FLAG");
    for row in 0..row_count {
        for idx_in_cell in 0..6 {
            let idx = idx_in_cell + row * 6;
            assert_eq!(flag_raw[idx] != 0, row % 2 == 1);
        }
    }

    let mut uvw_writer = StreamingTiledPrimitiveWriter::create_column(
        dir.path().join("UVW.tmp"),
        row_count,
        vec![3],
        vec![3, 2],
        StreamedTiledPrimitiveType::Float64,
        STREAMING_TILED_COLUMN_BUFFER_BYTES,
        false,
    )
    .expect("create UVW writer");
    for row in 0..row_count {
        uvw_writer
            .push_f64_row(&[row as f64, row as f64 + 10.0, row as f64 + 20.0])
            .expect("UVW row");
    }
    let streamed_uvw = uvw_writer.finish().expect("finish UVW");
    assert_eq!(streamed_uvw.bytes_written(), 144);
    install_streamed_tiled_column_primitive_column(&table_path, 1, "UVW", streamed_uvw)
        .expect("install UVW");

    let (uvw_variant, uvw_header) =
        read_tiled_header(&table_path.join("table.f1")).expect("read UVW header");
    assert!(matches!(uvw_variant, TiledVariant::Column { .. }));
    assert_eq!(uvw_header.cubes[0].cube_shape, vec![3, row_count]);
    let uvw_file = std::fs::read(tsm_data_path(&table_path, 1, 0)).expect("read UVW TSM");
    let (uvw_bucket_size, uvw_offsets) =
        compute_tile_layout(&uvw_header.col_data_types, &uvw_header.cubes[0].tile_shape);
    let uvw_raw = reconstruct_cube_column(
        &uvw_file,
        uvw_header.cubes[0].file_offset as usize,
        &uvw_header.cubes[0].cube_shape,
        &uvw_header.cubes[0].tile_shape,
        uvw_offsets[0],
        uvw_bucket_size,
        CasacoreDataType::TpDouble,
        8,
    )
    .expect("reconstruct UVW");
    for row in 0..row_count {
        for axis in 0..3 {
            let idx = axis + row * 3;
            let bytes = &uvw_raw[idx * 8..idx * 8 + 8];
            let value = f64::from_le_bytes(bytes.try_into().unwrap());
            assert_eq!(value, row as f64 + axis as f64 * 10.0);
        }
    }
}

#[test]
fn streamed_shape_primitive_writer_allows_zero_sized_cells() {
    let dir = tempdir().expect("tempdir");
    let table_path = dir.path().join("flag-category.table");
    let row_count = 5usize;
    let tile_shape = vec![2, 2, 1, 2];

    let mut writer = StreamingTiledPrimitiveWriter::create_shape(
        dir.path().join("FLAG_CATEGORY.tmp"),
        row_count,
        vec![0, 2, 3],
        tile_shape.clone(),
        StreamedTiledPrimitiveType::Bool,
        STREAMING_TILED_COLUMN_BUFFER_BYTES,
        false,
    )
    .expect("create FLAG_CATEGORY writer");
    for _ in 0..row_count {
        writer.push_zero_row().expect("zero-size row");
    }
    let streamed = writer.finish().expect("finish FLAG_CATEGORY");
    assert_eq!(streamed.bytes_written(), 0);
    install_streamed_tiled_shape_primitive_column(&table_path, 0, "FLAG_CATEGORY", streamed)
        .expect("install FLAG_CATEGORY");

    let data_path = tsm_data_path(&table_path, 0, 0);
    assert_eq!(std::fs::metadata(&data_path).expect("data file").len(), 0);
    let (variant, header) =
        read_tiled_header(&table_path.join("table.f0")).expect("read FLAG_CATEGORY header");
    let TiledVariant::Shape {
        default_tile_shape,
        nr_used_row_map,
        row_map,
        cube_map,
        pos_map,
    } = variant
    else {
        panic!("expected TiledShapeStMan header");
    };
    assert_eq!(default_tile_shape, vec![2, 2, 1, 2]);
    assert_eq!(nr_used_row_map, 1);
    assert_eq!(row_map, vec![(row_count - 1) as u32]);
    assert_eq!(cube_map, vec![1]);
    assert_eq!(pos_map, vec![(row_count - 1) as u32]);
    assert_eq!(header.nrrow, row_count as u64);
    assert_eq!(header.files[0].as_ref().expect("file").length, 0);
    assert_eq!(header.cubes[1].cube_shape, vec![0, 2, 3, row_count]);
    assert_eq!(header.cubes[1].tile_shape, tile_shape);
}

#[test]
fn table_cache_budget_api_starts_at_its_default_and_takes_an_override() {
    let _guard = shared_table_cache_test_guard();
    reset_table_cache_budget_for_tests();
    assert_eq!(table_cache_budget_bytes(), DEFAULT_TABLE_CACHE_BYTES);

    set_table_cache_budget_bytes(4096);
    assert_eq!(table_cache_budget_bytes(), 4096);

    reset_table_cache_budget_for_tests();
}

#[test]
fn typed_bool_cache_budget_counts_unpacked_tiles_on_create_and_reopen() {
    let root = tempdir().unwrap();
    let values = (0..65).map(|index| index % 3 == 0).collect::<Vec<_>>();
    let mut storage = TiledArrayStorage::create_with_cache::<bool>(
        root.path(),
        &[65],
        &[8],
        false,
        0,
        "DATA",
        16,
    )
    .unwrap();
    assert!(matches!(storage.inner.cache, TileCache::Lru(_)));
    storage.put_slice_c_order(&values, &[0], &[65]).unwrap();
    assert_eq!(storage.owned_resident_bytes(), 16);
    storage.flush().unwrap();
    assert_eq!(std::fs::metadata(&storage.inner.tsm_path).unwrap().len(), 9);
    drop(storage);

    let mut storage = TiledArrayStorage::open_with_cache::<bool>(root.path(), 0, 16).unwrap();
    assert!(matches!(storage.inner.cache, TileCache::Lru(_)));
    assert_eq!(
        storage
            .get_slice::<bool>(&[0], &[65])
            .unwrap()
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        values
    );
    assert_eq!(storage.owned_resident_bytes(), 16);
}

#[cfg(unix)]
#[test]
fn bounded_pixel_reads_refresh_with_the_original_open_file() {
    use std::os::unix::fs::PermissionsExt;

    for budget in [1, 1024] {
        let root = tempdir().unwrap();
        let mut writer = TiledArrayStorage::create_with_cache::<f32>(
            root.path(),
            &[4],
            &[2],
            false,
            0,
            "DATA",
            budget,
        )
        .unwrap();
        writer
            .put_slice_fortran(&[1.0f32, 2.0, 3.0, 4.0], &[0], &[4])
            .unwrap();
        writer.flush().unwrap();
        let file = writer.inner.tsm_path.to_path_buf();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o444)).unwrap();
        let mut reader = TiledArrayStorage::open_with_cache::<f32>(root.path(), 0, budget).unwrap();
        assert_eq!(reader.get_at::<f32>(&[0]).unwrap(), 1.0);
        if budget == 1 {
            assert_eq!(reader.owned_resident_bytes(), 2 * size_of::<f32>());
        }
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
        writer.put_slice_fortran(&[5.0f32], &[0], &[1]).unwrap();
        writer.discard_clean_read_cache();
        assert_eq!(writer.get_at::<f32>(&[0]).unwrap(), 5.0);
        writer.flush().unwrap();
        drop(writer);
        std::fs::rename(file, root.path().join("relocated-pixels")).unwrap();
        reader.discard_clean_read_cache();
        assert_eq!(reader.get_at::<f32>(&[0]).unwrap(), 5.0);
    }
}

#[test]
fn typed_scalar_reads_match_slices_across_tiles_endian_and_cache_modes() {
    let shape = [5, 7, 2, 1];
    let values = (0..70)
        .map(|index| num_complex::Complex32::new(index as f32, -(index as f32)))
        .collect::<Vec<_>>();
    for big_endian in [false, true] {
        for budget in [64, 4096] {
            let root = tempdir().unwrap();
            let mut storage = TiledArrayStorage::create_with_cache::<num_complex::Complex32>(
                root.path(),
                &shape,
                &[2, 3, 1, 1],
                big_endian,
                0,
                "scalar",
                budget,
            )
            .unwrap();
            storage
                .put_slice_fortran(&values, &[0, 0, 0, 0], &shape)
                .unwrap();
            storage.flush().unwrap();
            drop(storage);
            let mut storage = TiledArrayStorage::open_with_cache::<num_complex::Complex32>(
                root.path(),
                0,
                budget,
            )
            .unwrap();
            for x in 0..shape[0] {
                for y in 0..shape[1] {
                    for z in 0..shape[2] {
                        let position = [x, y, z, 0];
                        let expected = values[x + shape[0] * (y + shape[1] * z)];
                        let value = storage.get_at::<num_complex::Complex32>(&position).unwrap();
                        assert_eq!(value.re.to_bits(), expected.re.to_bits());
                        assert_eq!(value.im.to_bits(), expected.im.to_bits());
                        let slice = storage
                            .get_slice::<num_complex::Complex32>(&position, &[1; 4])
                            .unwrap();
                        assert_eq!(value, slice[[0, 0, 0, 0]]);
                    }
                }
            }
            assert!(storage.get_at::<f32>(&[0; 4]).is_err());
            for invalid in [
                &[0, 0, 0][..],
                &[5, 0, 0, 0],
                &[0, 7, 0, 0],
                &[0, 0, 2, 0],
                &[0, 0, 0, 1],
                &[usize::MAX, 0, 0, 0],
            ] {
                assert!(storage.get_at::<num_complex::Complex32>(invalid).is_err());
            }
        }
    }
}

#[test]
fn bool_lru_eviction_round_trips_after_reopen() {
    let dir = tempdir().unwrap();
    let table_path = dir.path().join("bool_eviction.table");
    std::fs::create_dir_all(&table_path).unwrap();
    let mut io = TiledFileIO::create_with_cache_limit(
        &table_path,
        &[8],
        &[4],
        PrimitiveType::Bool,
        false,
        0,
        "bool_eviction",
        1,
    )
    .unwrap();

    let first_tile = make_bool_tile_pattern(0);
    let second_tile = make_bool_tile_pattern(4);
    io.put_slice_fortran(&first_tile, &[0], &[4]).unwrap();
    io.put_slice_fortran(&second_tile, &[4], &[4]).unwrap();
    io.flush().unwrap();
    drop(io);

    let mut reopened = TiledFileIO::open_with_cache_limit(&table_path, 0, 1).unwrap();
    let all = reopened.get_all::<bool>().unwrap();
    let expected: Vec<bool> = first_tile.into_iter().chain(second_tile).collect();
    assert_eq!(all.iter().copied().collect::<Vec<_>>(), expected);
}

#[test]
fn bool_lru_flush_keeps_tiles_readable_in_same_session() {
    let dir = tempdir().unwrap();
    let table_path = dir.path().join("bool_flush.table");
    std::fs::create_dir_all(&table_path).unwrap();
    let mut io = TiledFileIO::create_with_cache_limit(
        &table_path,
        &[8],
        &[4],
        PrimitiveType::Bool,
        false,
        0,
        "bool_flush",
        1,
    )
    .unwrap();

    let first_tile = make_bool_tile_pattern(0);
    let second_tile = make_bool_tile_pattern(4);
    io.put_slice_fortran(&first_tile, &[0], &[4]).unwrap();
    io.put_slice_fortran(&second_tile, &[4], &[4]).unwrap();

    io.flush().unwrap();

    let all = io.get_all::<bool>().unwrap();
    let expected: Vec<bool> = first_tile.into_iter().chain(second_tile).collect();
    assert_eq!(all.iter().copied().collect::<Vec<_>>(), expected);
}

#[test]
fn bool_flat_flush_keeps_tiles_readable_in_same_session() {
    let dir = tempdir().unwrap();
    let table_path = dir.path().join("bool_flat_flush.table");
    std::fs::create_dir_all(&table_path).unwrap();
    let mut io = TiledFileIO::create(
        &table_path,
        &[8],
        &[4],
        PrimitiveType::Bool,
        false,
        0,
        "bool_flat_flush",
    )
    .unwrap();

    let first_tile = make_bool_tile_pattern(0);
    let second_tile = make_bool_tile_pattern(4);
    io.put_slice_fortran(&first_tile, &[0], &[4]).unwrap();
    io.put_slice_fortran(&second_tile, &[4], &[4]).unwrap();

    io.flush().unwrap();

    let all = io.get_all::<bool>().unwrap();
    let expected: Vec<bool> = first_tile.into_iter().chain(second_tile).collect();
    assert_eq!(all.iter().copied().collect::<Vec<_>>(), expected);
}

#[test]
fn uniform_boolean_table_initializes_padded_tiles() {
    let dir = tempdir().unwrap();
    let layout = TiledArrayStorageLayout::new(
        &[5, 7],
        &[3, 5],
        PrimitiveType::Bool,
        false,
        0,
        "values",
        64,
    )
    .unwrap();
    for value in [false, true] {
        let path = dir.path().join(value.to_string());
        TiledArrayStorage::create_filled_bool_table(&path, &layout, value).unwrap();
        let mut storage = TiledArrayStorage::open::<bool>(&path, 0).unwrap();
        let data = storage.get_slice::<bool>(&[0, 0], &[5, 7]).unwrap();
        assert!(data.iter().all(|&pixel| pixel == value));
        storage
            .put_slice_fortran(&[!value], &[4, 6], &[1, 1])
            .unwrap();
        storage.flush().unwrap();
        let mut reopened = TiledArrayStorage::open::<bool>(&path, 0).unwrap();
        let mut expected = ArrayD::from_elem(IxDyn(&[5, 7]), value);
        expected[[4, 6]] = !value;
        assert_eq!(
            reopened.get_slice::<bool>(&[0, 0], &[5, 7]).unwrap(),
            expected
        );
    }
}

#[test]
fn uniform_boolean_table_uses_canonical_values_for_single_pixel_tiles() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("mask");
    let layout =
        TiledArrayStorageLayout::new(&[3], &[1], PrimitiveType::Bool, false, 0, "values", 4)
            .unwrap();
    TiledArrayStorage::create_filled_bool_table(&path, &layout, true).unwrap();
    assert_eq!(std::fs::read(tsm_data_path(&path, 0, 0)).unwrap(), [1; 3]);
}

#[test]
fn flat_flush_returns_final_buffered_write_failure_and_retains_dirty_data() {
    let dir = tempdir().unwrap();
    let mut storage = TiledArrayStorage::create_planned_table(
        &dir.path().join("failed_flush"),
        &TiledArrayStorageLayout::new(
            &[2, 2, 1],
            &[2, 2, 1],
            PrimitiveType::Float32,
            cfg!(target_endian = "big"),
            0,
            "values",
            16,
        )
        .unwrap(),
    )
    .unwrap();
    let values = [1.0_f32, 2.0, 3.0, 4.0];
    storage
        .put_slice_fortran(&values, &[0, 0, 0], &[2, 2, 1])
        .unwrap();
    // Seek succeeds on a read-only descriptor. The 16-byte write fits in
    // BufWriter and only fails when its final buffered bytes are flushed.
    assert!(
        storage
            .inner
            .flush_with_flat_file(|path| File::open(path))
            .is_err()
    );
    let TileCache::Flat(cache) = &storage.inner.cache else {
        panic!("expected flat cache")
    };
    assert!(cache.allocated && cache.dirty.iter().any(|dirty| *dirty));
    storage.flush().unwrap();
    let mut actual = [0.0; 4];
    storage
        .get_slice_into(&[0, 0, 0], &[2, 2, 1], &mut actual)
        .unwrap();
    assert_eq!(actual, values);
}

#[test]
fn linear_to_nd_round_trip() {
    let dims = [3, 4, 2];
    for i in 0..24 {
        let pos = linear_to_nd(i, &dims);
        assert_eq!(nd_to_linear(&pos, &dims), i);
    }
}

#[test]
fn compute_tile_layout_single_column() {
    let types = [CasacoreDataType::TpFloat];
    let tile = [4, 8];
    let (bucket, offsets) = compute_tile_layout(&types, &tile);
    assert_eq!(offsets[0], 0);
    assert_eq!(bucket, 4 * 8 * 4); // 128 bytes
}

#[test]
fn compute_tile_layout_two_columns() {
    let types = [CasacoreDataType::TpFloat, CasacoreDataType::TpDouble];
    let tile = [2, 3];
    let nrpixels = 6;
    let (bucket, offsets) = compute_tile_layout(&types, &tile);
    assert_eq!(offsets[0], 0);
    assert_eq!(offsets[1], nrpixels * 4);
    assert_eq!(bucket, nrpixels * 4 + nrpixels * 8);
}

#[test]
fn copy_tile_to_cube_1d() {
    let tile = [1u8, 2, 3, 4];
    let mut cube = [0u8; 6];
    copy_tile_to_cube(&tile, &[4], &mut cube, &[6], &[1], &[4], 1);
    assert_eq!(cube, [0, 1, 2, 3, 4, 0]);
}

#[test]
fn copy_tile_cube_round_trip_2d() {
    // Cube 4x6, tile 3x4.
    let cube_shape = [4, 6];
    let tile_shape = [3, 4];

    // Fill a cube with sequential values.
    let cube_nelem = 24;
    let cube_data: Vec<u8> = (0..cube_nelem as u8).collect();

    // Tile at grid position (0,0) covers cube[0..3, 0..4].
    let mut tile = [0u8; 12]; // 3*4=12
    copy_cube_to_tile(
        &cube_data,
        &cube_shape,
        &mut tile,
        &tile_shape,
        &[0, 0],
        &[3, 4],
        1,
    );

    let mut reconstructed = [0u8; 24];
    copy_tile_to_cube(
        &tile,
        &tile_shape,
        &mut reconstructed,
        &cube_shape,
        &[0, 0],
        &[3, 4],
        1,
    );

    // Only the [0..3, 0..4] region should match.
    for j in 0..4 {
        for i in 0..3 {
            let idx = i + j * 4;
            assert_eq!(reconstructed[idx], cube_data[idx], "mismatch at ({i},{j})");
        }
    }
}

mod selected_reads;
