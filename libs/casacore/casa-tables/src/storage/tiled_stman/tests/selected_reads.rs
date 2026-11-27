// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn tiled_selected_row_reads_reuse_shared_tile_cache() {
    let _guard = shared_table_cache_test_guard();
    reset_table_cache_budget_for_tests();
    set_table_cache_budget_bytes(128);

    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float32,
        vec![2, 2],
    )])
    .expect("schema");
    let mut table = Table::with_schema(schema);
    for row_idx in 0..6 {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(
                        vec![2, 2],
                        vec![
                            row_idx as f32,
                            row_idx as f32 + 10.0,
                            row_idx as f32 + 20.0,
                            row_idx as f32 + 30.0,
                        ],
                    )
                    .expect("shape data"),
                )),
            )]))
            .expect("push row");
    }

    let dir = tempdir().expect("tempdir");
    let root = dir.path().join("shared_tile_cache.table");
    std::fs::create_dir_all(&root).expect("create test dir");
    table
        .save(
            TableOptions::new(&root)
                .with_data_manager(DataManagerKind::TiledShapeStMan)
                .with_tile_shape(vec![2, 2, 2]),
        )
        .expect("save tiled-shape table");

    let reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
    assert_eq!(shared_tile_cache_entry_count_for_table(&root), 0);
    reopened
        .get_array_cells_owned("data", &[0, 1, 4, 5])
        .expect("first tiled selected-row read");
    let cache_entries_after_first_read = shared_tile_cache_entry_count_for_table(&root);
    assert!(
        cache_entries_after_first_read > 0,
        "first tiled selected-row read should populate the shared tile cache"
    );

    reopened
        .get_array_cells_owned("data", &[0, 1, 4, 5])
        .expect("second tiled selected-row read");
    assert_eq!(
        shared_tile_cache_entry_count_for_table(&root),
        cache_entries_after_first_read,
        "repeated tiled selected-row reads should reuse the shared tile cache"
    );

    reset_table_cache_budget_for_tests();
}

/// A table handle can outlive a rewrite at its path. A stale handle's
/// cached bytes must not be mistaken for unpacked bools by the new one.
#[test]
fn selected_bool_reads_do_not_reuse_tiles_from_stale_metadata() {
    let _guard = shared_table_cache_test_guard();
    reset_table_cache_budget_for_tests();

    let write_table = |root: &Path, primitive, rows, tile_rows| {
        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            "data",
            primitive,
            vec![1, 1],
        )])
        .expect("schema");
        let mut table = Table::with_schema(schema);
        for _ in 0..rows {
            let array = match primitive {
                PrimitiveType::Bool => ArrayValue::Bool(ArrayD::from_elem(IxDyn(&[1, 1]), true)),
                PrimitiveType::Float32 => {
                    ArrayValue::Float32(ArrayD::from_elem(IxDyn(&[1, 1]), 1.0))
                }
                _ => unreachable!(),
            };
            table
                .add_row(RecordValue::new(vec![RecordField::new(
                    "data",
                    Value::Array(array),
                )]))
                .expect("row");
        }
        table
            .save(
                TableOptions::new(root)
                    .with_data_manager(DataManagerKind::TiledShapeStMan)
                    .with_tile_shape(vec![1, 1, tile_rows]),
            )
            .expect("save");
    };

    let dir = tempdir().expect("tempdir");
    for (index, old_type, old_tile, new_tile, rows) in [
        (0, PrimitiveType::Float32, 8, 8, 256),
        (1, PrimitiveType::Bool, 1, 2, 2),
    ] {
        let root = dir.path().join(format!("stale-{index}.table"));
        std::fs::create_dir_all(&root).expect("table directory");
        write_table(&root, old_type, rows, old_tile);
        let old = Table::open(TableOptions::new(&root)).expect("old handle");
        write_table(&root, PrimitiveType::Bool, rows, new_tile);
        let current = Table::open(TableOptions::new(&root)).expect("new handle");

        // Read through the old handle after the new open invalidates the
        // cache, then read the same tile through the new handle.
        let mut old_floats = Vec::new();
        let mut old_bools = Vec::new();
        let destination = match old_type {
            PrimitiveType::Float32 => SelectedArray2DCellsMut::Float32(&mut old_floats),
            PrimitiveType::Bool => SelectedArray2DCellsMut::Bool(&mut old_bools),
            _ => unreachable!(),
        };
        old.fill_array_cells_2d_channel_range_typed_uncached("data", &[0], 0, 1, destination)
            .expect("stale read")
            .expect("defined stale cell");
        let mut flags = Vec::new();
        current
            .fill_array_cells_2d_channel_range_typed_uncached(
                "data",
                &[0],
                0,
                1,
                SelectedArray2DCellsMut::Bool(&mut flags),
            )
            .expect("current read")
            .expect("defined current cell");
        assert_eq!(flags, [true], "stale case {index}");
    }
    reset_table_cache_budget_for_tests();
}

#[test]
fn tiled_selected_channel_range_reads_only_intersecting_channel_tiles() {
    let _guard = shared_table_cache_test_guard();
    reset_table_cache_budget_for_tests();
    set_table_cache_budget_bytes(1024 * 1024);

    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "DATA",
        PrimitiveType::Complex32,
        vec![2, 8],
    )])
    .expect("schema");
    let mut table = Table::with_schema(schema);
    for row_idx in 0..4 {
        let mut values = Vec::new();
        for channel in 0..8 {
            for corr in 0..2 {
                let value = row_idx as f32 * 100.0 + channel as f32 * 10.0 + corr as f32;
                values.push(Complex32::new(value, -value));
            }
        }
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "DATA",
                Value::Array(ArrayValue::Complex32(
                    ArrayD::from_shape_vec(ndarray::IxDyn(&[2, 8]).f(), values)
                        .expect("shape DATA"),
                )),
            )]))
            .expect("push row");
    }

    let dir = tempdir().expect("tempdir");
    let root = dir.path().join("channel_range_tile_subset.table");
    std::fs::create_dir_all(&root).expect("create test dir");
    table
        .save(
            TableOptions::new(&root)
                .with_data_manager(DataManagerKind::TiledShapeStMan)
                .with_tile_shape(vec![2, 2, 2]),
        )
        .expect("save tiled-shape table");

    let reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
    assert_eq!(shared_tile_cache_entry_count_for_table(&root), 0);

    let selected = reopened
        .get_array_cells_2d_channel_range_typed_uncached("DATA", &[1], 2, 2)
        .expect("selected channel-range read")
        .expect("defined selected cells");
    let SelectedArray2DCells::Complex32(selected) = selected else {
        panic!("expected Complex32 selected channel-range cells");
    };
    assert_eq!(selected.row_count(), 1);
    assert_eq!(selected.axis0_count(), 2);
    assert_eq!(selected.channel_count(), 2);
    assert_eq!(
        selected.values(),
        &[
            Complex32::new(120.0, -120.0),
            Complex32::new(121.0, -121.0),
            Complex32::new(130.0, -130.0),
            Complex32::new(131.0, -131.0),
        ],
    );
    assert_eq!(
        shared_tile_cache_entry_count_for_table(&root),
        1,
        "channel range 2..4 should touch only one channel tile"
    );

    invalidate_shared_tile_cache_for_table(&root);
    assert_eq!(shared_tile_cache_entry_count_for_table(&root), 0);
    reopened
        .get_array_cells_owned("DATA", &[1])
        .expect("full selected-row read");
    assert_eq!(
        shared_tile_cache_entry_count_for_table(&root),
        4,
        "full 8-channel row should touch all four channel tiles for this shape"
    );

    reset_table_cache_budget_for_tests();
}

#[test]
fn sparse_uniform_reads_reuse_tiles_and_dense_reads_stream() {
    let _guard = shared_table_cache_test_guard();
    reset_table_cache_budget_for_tests();
    set_table_cache_budget_bytes(1024 * 1024);
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "FLAG",
        PrimitiveType::Bool,
        vec![2, 4],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);
    for row in 0..96 {
        let value = ArrayValue::Bool(ArrayD::from_elem(IxDyn(&[2, 4]).f(), row % 16 == 0));
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "FLAG",
                Value::Array(value),
            )]))
            .unwrap();
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("uniform.table");
    std::fs::create_dir(&path).unwrap();
    table
        .save(
            TableOptions::new(&path)
                .with_data_manager(DataManagerKind::TiledShapeStMan)
                .with_tile_shape(vec![2, 4, 8]),
        )
        .unwrap();
    let table = Table::open(TableOptions::new(&path)).unwrap();
    let flags = table.column_accessor("FLAG").unwrap();
    let sparse = (0..96).step_by(8).collect::<Vec<_>>();
    let ArrayValue::Bool(first) = flags.packed_2d_cells_uncached(&sparse).unwrap().unwrap() else {
        panic!("expected packed Boolean flags");
    };
    assert_eq!(first.shape(), &[2, 4, 12]);
    assert!(first[[1, 2, 0]] && !first[[1, 2, 1]]);
    assert_eq!(shared_tile_cache_entry_count_for_table(&path), 0);
    for _ in 0..2 {
        assert_eq!(
            flags.packed_2d_cells_uncached(&sparse).unwrap(),
            Some(ArrayValue::Bool(first.clone()))
        );
        assert_eq!(shared_tile_cache_entry_count_for_table(&path), 12);
    }

    invalidate_shared_tile_cache_for_table(&path);
    let dense = (0..96).collect::<Vec<_>>();
    assert!(flags.packed_2d_cells_uncached(&dense).unwrap().is_some());
    assert_eq!(shared_tile_cache_entry_count_for_table(&path), 0);
    reset_table_cache_budget_for_tests();
}

/// A tile can pad its correlation axis beyond the cell's correlations.
#[test]
fn selected_channel_copy_handles_padded_correlation_tiles() {
    let _guard = shared_table_cache_test_guard();
    reset_table_cache_budget_for_tests();
    for primitive in [PrimitiveType::Complex32, PrimitiveType::Bool] {
        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            "DATA",
            primitive,
            vec![2, 6],
        )])
        .expect("schema");
        let mut table = Table::with_schema(schema);
        for row in 0..5 {
            let values =
                (0..6).flat_map(|channel| (0..2).map(move |corr| row * 100 + channel * 10 + corr));
            let array = match primitive {
                PrimitiveType::Complex32 => ArrayValue::Complex32(
                    ArrayD::from_shape_vec(
                        IxDyn(&[2, 6]).f(),
                        values
                            .map(|value| Complex32::new(value as f32, -(value as f32)))
                            .collect(),
                    )
                    .expect("values"),
                ),
                PrimitiveType::Bool => ArrayValue::Bool(
                    ArrayD::from_shape_vec(
                        IxDyn(&[2, 6]).f(),
                        values.map(|value| value % 3 == 0).collect(),
                    )
                    .expect("flags"),
                ),
                _ => unreachable!(),
            };
            table
                .add_row(RecordValue::new(vec![RecordField::new(
                    "DATA",
                    Value::Array(array),
                )]))
                .expect("row");
        }
        let dir = tempdir().expect("tempdir");
        let root = dir.path().join("padded.table");
        std::fs::create_dir_all(&root).expect("directory");
        table
            .save(
                TableOptions::new(&root)
                    .with_data_manager(DataManagerKind::TiledShapeStMan)
                    .with_tile_shape(vec![4, 2, 2]),
            )
            .expect("save");
        let (_, header) = read_tiled_header(&root.join("table.f0")).expect("header");
        assert!(
            header.cubes.iter().any(|cube| {
                cube.cube_shape.first() == Some(&2) && cube.tile_shape == [4, 2, 2]
            }),
            "the correlation tile is padded"
        );
        let reopened = Table::open(TableOptions::new(&root)).expect("open");
        let rows = [4, 0, 2, 0];
        let mut data = Vec::new();
        let mut flags = Vec::new();
        let destination = match primitive {
            PrimitiveType::Complex32 => SelectedArray2DCellsMut::Complex32(&mut data),
            PrimitiveType::Bool => SelectedArray2DCellsMut::Bool(&mut flags),
            _ => unreachable!(),
        };
        reopened
            .fill_array_cells_2d_channel_range_typed_uncached("DATA", &rows, 1, 4, destination)
            .expect("read")
            .expect("defined cells");
        for channel in 1..5 {
            for (index, row) in rows.iter().enumerate() {
                for corr in 0..2 {
                    let value = row * 100 + channel * 10 + corr;
                    let output = ((channel - 1) * rows.len() + index) * 2 + corr;
                    match primitive {
                        PrimitiveType::Complex32 => {
                            assert_eq!(data[output], Complex32::new(value as f32, -(value as f32)))
                        }
                        PrimitiveType::Bool => assert_eq!(flags[output], value % 3 == 0),
                        _ => unreachable!(),
                    }
                }
            }
        }
    }
    reset_table_cache_budget_for_tests();
}

#[test]
fn streamed_selected_channels_bound_physical_read_ahead() {
    let _guard = shared_table_cache_test_guard();
    reset_table_cache_budget_for_tests();
    set_table_cache_budget_bytes(1024 * 1024);

    for endian in [
        crate::EndianFormat::BigEndian,
        crate::EndianFormat::LittleEndian,
    ] {
        for primitive in [
            PrimitiveType::Complex32,
            PrimitiveType::Float32,
            PrimitiveType::Bool,
        ] {
            let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
                "DATA",
                primitive,
                vec![2, 12],
            )])
            .unwrap();
            let mut table = Table::with_schema(schema);
            for row in 0..56 {
                let values = (0..12)
                    .flat_map(|channel| (0..2).map(move |corr| row * 100 + channel * 10 + corr));
                let array = match primitive {
                    PrimitiveType::Complex32 => ArrayValue::Complex32(
                        ArrayD::from_shape_vec(
                            ndarray::IxDyn(&[2, 12]).f(),
                            values
                                .map(|value| Complex32::new(value as f32, -(value as f32)))
                                .collect(),
                        )
                        .unwrap(),
                    ),
                    PrimitiveType::Float32 => ArrayValue::Float32(
                        ArrayD::from_shape_vec(
                            ndarray::IxDyn(&[2, 12]).f(),
                            values.map(|value| value as f32).collect(),
                        )
                        .unwrap(),
                    ),
                    PrimitiveType::Bool => ArrayValue::Bool(
                        ArrayD::from_shape_vec(
                            ndarray::IxDyn(&[2, 12]).f(),
                            values.map(|value| value % 3 == 0).collect(),
                        )
                        .unwrap(),
                    ),
                    _ => unreachable!(),
                };
                table
                    .add_row(RecordValue::new(vec![RecordField::new(
                        "DATA",
                        Value::Array(array),
                    )]))
                    .unwrap();
            }
            let dir = tempdir().unwrap();
            let root = dir.path().join("selected_read_ahead.table");
            std::fs::create_dir_all(&root).unwrap();
            table
                .save(
                    TableOptions::new(&root)
                        .with_data_manager(DataManagerKind::TiledShapeStMan)
                        .with_tile_shape(vec![2, 2, 2])
                        .with_endian_format(endian),
                )
                .unwrap();
            let reopened = Table::open(TableOptions::new(&root)).unwrap();
            // Fourteen nonadjacent row tiles force the production streaming
            // path. Duplicate and reversed rows must not duplicate reads.
            let mut rows: Vec<_> = (0..56).step_by(4).rev().collect();
            rows.push(0);
            for (start, count, row_major) in [
                (2, 2, false),
                (1, 2, false),
                (11, 1, false),
                (0, 12, false),
                (2, 2, true),
                (1, 2, true),
                (11, 1, true),
                (0, 12, true),
            ] {
                let mut data = Vec::new();
                let mut floats = Vec::new();
                let mut flags = Vec::new();
                let destination = match (primitive, row_major) {
                    (PrimitiveType::Complex32, false) => {
                        SelectedArray2DCellsMut::Complex32(&mut data)
                    }
                    (PrimitiveType::Complex32, true) => {
                        SelectedArray2DCellsMut::RowChannelComplex32(&mut data)
                    }
                    (PrimitiveType::Float32, false) => {
                        SelectedArray2DCellsMut::Float32(&mut floats)
                    }
                    (PrimitiveType::Float32, true) => {
                        SelectedArray2DCellsMut::RowChannelFloat32(&mut floats)
                    }
                    (PrimitiveType::Bool, false) => SelectedArray2DCellsMut::Bool(&mut flags),
                    (PrimitiveType::Bool, true) => {
                        SelectedArray2DCellsMut::RowChannelBool(&mut flags)
                    }
                    _ => unreachable!(),
                };
                STREAMED_READ_COUNTS.with(|counts| counts.set((0, 0)));
                reopened
                    .fill_array_cells_2d_channel_range_typed_uncached(
                        "DATA",
                        &rows,
                        start,
                        count,
                        destination,
                    )
                    .unwrap()
                    .expect("defined cells");
                let (calls, bytes) = STREAMED_READ_COUNTS.with(|counts| counts.get());
                let channel_tiles = (start + count - 1) / 2 - start / 2 + 1;
                let tile_bytes = match primitive {
                    PrimitiveType::Bool => 1,
                    PrimitiveType::Float32 => 32,
                    PrimitiveType::Complex32 => 64,
                    _ => unreachable!(),
                };
                assert_eq!(
                    bytes,
                    14 * channel_tiles * tile_bytes,
                    "read only selected tile runs: {primitive:?} {endian:?} {start} {count}"
                );
                assert_eq!(calls, 14, "read each selected run as a block");
                for channel in start..start + count {
                    for (index, row) in rows.iter().enumerate() {
                        for corr in 0..2 {
                            let value = row * 100 + channel * 10 + corr;
                            let output = if row_major {
                                (index * count + channel - start) * 2 + corr
                            } else {
                                ((channel - start) * rows.len() + index) * 2 + corr
                            };
                            match primitive {
                                PrimitiveType::Complex32 => assert_eq!(
                                    data[output],
                                    Complex32::new(value as f32, -(value as f32))
                                ),
                                PrimitiveType::Float32 => {
                                    assert_eq!(floats[output], value as f32)
                                }
                                PrimitiveType::Bool => {
                                    assert_eq!(flags[output], value % 3 == 0)
                                }
                                _ => unreachable!(),
                            }
                        }
                    }
                }
            }
            assert_eq!(shared_tile_cache_entry_count_for_table(&root), 0);
        }
    }
    reset_table_cache_budget_for_tests();
}

#[test]
fn large_typed_channel_range_uses_streaming_read_ahead() {
    let _guard = shared_table_cache_test_guard();
    reset_table_cache_budget_for_tests();
    set_table_cache_budget_bytes(1024 * 1024);

    let row_count = 12usize;
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("DATA", PrimitiveType::Complex32, vec![2, 8]),
        ColumnSchema::array_fixed("FLAG", PrimitiveType::Bool, vec![2, 8]),
    ])
    .expect("schema");
    let mut table = Table::with_schema(schema);
    for row_idx in 0..row_count {
        let values = (0..8)
            .flat_map(|channel| {
                (0..2).map(move |corr| {
                    let value = row_idx as f32 * 100.0 + channel as f32 * 10.0 + corr as f32;
                    Complex32::new(value, -value)
                })
            })
            .collect::<Vec<_>>();
        let flags = (0..8)
            .flat_map(|channel| (0..2).map(move |corr| (row_idx + channel + corr) % 2 == 0))
            .collect::<Vec<_>>();
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "DATA",
                    Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(ndarray::IxDyn(&[2, 8]).f(), values)
                            .expect("shape DATA"),
                    )),
                ),
                RecordField::new(
                    "FLAG",
                    Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(ndarray::IxDyn(&[2, 8]).f(), flags)
                            .expect("shape FLAG"),
                    )),
                ),
            ]))
            .expect("push row");
    }

    let dir = tempdir().expect("tempdir");
    let root = dir.path().join("streamed_channel_range.table");
    std::fs::create_dir_all(&root).expect("create test dir");
    table
        .save(
            TableOptions::new(&root)
                .with_data_manager(DataManagerKind::TiledShapeStMan)
                .with_tile_shape(vec![2, 2, 1]),
        )
        .expect("save tiled-shape table");

    let reopened = Table::open(TableOptions::new(&root)).expect("open lazy table");
    let rows = (0..row_count).collect::<Vec<_>>();
    let selected = reopened
        .get_array_cells_2d_channel_range_typed_uncached("DATA", &rows, 1, 6)
        .expect("streamed channel-range read")
        .expect("defined selected cells");
    let SelectedArray2DCells::Complex32(selected) = selected else {
        panic!("expected Complex32 selected channel-range cells");
    };
    assert_eq!(selected.values().len(), row_count * 2 * 6);
    let channel = 3usize;
    let row = 7usize;
    let corr = 1usize;
    let output_channel = channel - 1;
    let output_index = output_channel * row_count * 2 + row * 2 + corr;
    assert_eq!(
        selected.values()[output_index],
        Complex32::new(731.0, -731.0)
    );
    let flags = reopened
        .get_array_cells_2d_channel_range_typed_uncached("FLAG", &rows, 1, 6)
        .expect("streamed flag channel-range read")
        .expect("defined selected flags");
    let SelectedArray2DCells::Bool(flags) = flags else {
        panic!("expected Bool selected channel-range cells");
    };
    assert!(!flags.values()[output_index]);
    assert!(flags.values()[output_index - 1]);
    assert_eq!(
        shared_tile_cache_entry_count_for_table(&root),
        0,
        "large one-pass reads should bypass the shared random-access tile cache"
    );

    reset_table_cache_budget_for_tests();
}
