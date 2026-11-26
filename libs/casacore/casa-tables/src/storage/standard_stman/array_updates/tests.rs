// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sparse replacements preserve aliases and every byte outside their offsets.

use super::*;
use crate::schema::ColumnSchema;
use casa_core::{PrimitiveType, RecordField, RecordValue};

fn address(header: &SsmHeader, index: &SsmIndex, column: u32, row: usize) -> usize {
    let (bucket, start, _) = index.find_bucket(row as u64).unwrap();
    bucket_offset(bucket as usize, header.bucket_size as usize) as usize
        + column as usize
        + (row as u64 - start) as usize * 8
}

#[test]
fn replacements_preserve_aliases_undefined_cells_and_other_bucket_bytes() {
    for (big_endian, undefined_row) in [(false, 2000), (true, 2000), (false, 3990), (true, 3990)] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("table.f0");
        let array_path = root.path().join("table.f0i");
        let descs = [
            ColumnSchema::array_variable("A", PrimitiveType::Float32, Some(1)),
            ColumnSchema::array_variable("B", PrimitiveType::Float32, Some(1)),
            ColumnSchema::scalar("SCAN", PrimitiveType::Int32),
        ]
        .iter()
        .map(ColumnDescContents::from_column_schema)
        .collect::<Vec<_>>();
        let records = (0..4001)
            .map(|row| {
                RecordValue::new(vec![
                    RecordField::new(
                        "A",
                        Value::Array(ArrayValue::from_f32_vec(vec![row as f32, 1.])),
                    ),
                    RecordField::new(
                        "B",
                        Value::Array(ArrayValue::from_f32_vec(vec![2., row as f32])),
                    ),
                    RecordField::new("SCAN", Value::Scalar(ScalarValue::Int32(row))),
                ])
            })
            .collect::<Vec<_>>();
        let blob = write_ssm_file(&path, &descs, &records, big_endian).unwrap();
        let mut file = File::open(&path).unwrap();
        let header = parse_ssm_header(&mut file).unwrap();
        let indices = parse_ssm_indices(&mut file, &header).unwrap();
        let dm = parse_ssm_dm_blob(&blob).unwrap();
        let at = |col: usize, row| {
            address(
                &header,
                &indices[dm.col_index_map[col] as usize],
                dm.column_offsets[col],
                row,
            )
        };
        // Simulate a second cell sharing the old A[1] record. Replacing A[1]
        // must leave B[3999] pointing at its original values.
        let mut before = std::fs::read(&path).unwrap();
        let source = before[at(0, 1)..at(0, 1) + 8].to_vec();
        before[at(1, 3999)..at(1, 3999) + 8].copy_from_slice(&source);
        std::fs::write(&path, &before).unwrap();
        let arrays_before = std::fs::read(&array_path).unwrap();
        let replacement = ArrayValue::from_f32_vec(vec![-1., 5., 7.]);
        assert!(
            save_ssm_array_rows_in_place(
                &path,
                &blob,
                &descs,
                vec![
                    Some(vec![(1, Some(replacement.clone())), (undefined_row, None)]),
                    None,
                    None,
                ],
                big_endian
            )
            .unwrap()
        );
        let after = std::fs::read(&path).unwrap();
        assert_eq!(before.len(), after.len());
        for (offset, (a, b)) in before.iter().zip(&after).enumerate() {
            if !(at(0, 1)..at(0, 1) + 8).contains(&offset)
                && !(at(0, undefined_row)..at(0, undefined_row) + 8).contains(&offset)
            {
                assert_eq!(a, b, "unmodified bucket byte {offset}");
            }
        }
        let arrays_after = std::fs::read(&array_path).unwrap();
        assert_eq!(&arrays_before[12..], &arrays_after[12..arrays_before.len()]);
        let refs = descs.iter().collect::<Vec<_>>();
        let columns = read_ssm_file(&path, &blob, &refs, records.len()).unwrap();
        for (column, (_, values)) in columns.into_iter().take(2).enumerate() {
            let SsmColumnResult::Indirect(values) = values else {
                panic!("indirect")
            };
            for (row, found) in values.into_iter().enumerate() {
                let expected = match (column, row) {
                    (0, 1) => Some(Value::Array(replacement.clone())),
                    (0, row) if row == undefined_row => None,
                    (1, 3999) => records[1].get("A").cloned(),
                    _ => records[row]
                        .get(if column == 0 { "A" } else { "B" })
                        .cloned(),
                };
                assert_eq!(found, expected, "column {column}, row {row}");
            }
        }
        // A layout we do not patch must return before changing either file.
        let mut unsupported = arrays_after.clone();
        unsupported[..4].copy_from_slice(&if big_endian {
            1u32.to_be_bytes()
        } else {
            1u32.to_le_bytes()
        });
        std::fs::write(&array_path, &unsupported).unwrap();
        assert!(
            !save_ssm_array_rows_in_place(
                &path,
                &blob,
                &descs,
                vec![Some(vec![(1, Some(replacement))]), None, None,],
                big_endian
            )
            .unwrap()
        );
        assert_eq!(std::fs::read(&path).unwrap(), after);
        assert_eq!(std::fs::read(&array_path).unwrap(), unsupported);
    }
}
