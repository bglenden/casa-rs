// SPDX-License-Identifier: LGPL-3.0-or-later
//! Append independently encoded TiledShapeStMan hypercubes without rewriting
//! the existing data files or decoding the appended cells.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs;

use super::{
    Path, PathBuf, StorageError, TiledStManHeader, TiledVariant, TsmFileInfo, Value,
    read_tiled_header, tsm_data_path, write_tiled_header,
};
use crate::storage::table_control::{self, ColumnDescContents, TableDatContents};

pub(crate) struct ShapeAppend {
    target: PathBuf,
    first_row: u64,
    row_count: u64,
    managers: BTreeMap<u32, Manager>,
}

struct Manager {
    column: String,
    variant: TiledVariant,
    header: TiledStManHeader,
    files: Vec<(PathBuf, u32, u64)>,
}

impl ShapeAppend {
    pub(crate) fn empty(target: &Path, first_row: usize, row_count: usize) -> Self {
        Self {
            target: target.into(),
            first_row: first_row as u64,
            row_count: row_count as u64,
            managers: BTreeMap::new(),
        }
    }

    pub(crate) fn plan(
        target: &Path,
        donor: &Path,
        columns: &[&str],
    ) -> Result<Self, StorageError> {
        let target_dat = table_control::read_table_dat(&target.join("table.dat"))?;
        let donor_dat = table_control::read_table_dat(&donor.join("table.dat"))?;
        let payload_names = fs::read_dir(target)?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<std::io::Result<BTreeSet<_>>>()?;
        let mut managers = BTreeMap::new();
        for &column in columns {
            if let Some((sequence, manager)) = prepare_manager(
                (target, &target_dat),
                (donor, &donor_dat),
                column,
                &payload_names,
            )? {
                managers.insert(sequence, manager);
            }
        }
        Ok(Self {
            target: target.into(),
            first_row: target_dat.nrrow,
            row_count: donor_dat.nrrow,
            managers,
        })
    }

    pub(crate) fn columns(&self) -> impl Iterator<Item = &str> {
        self.managers
            .values()
            .map(|manager| manager.column.as_str())
    }

    pub(crate) fn validate(&self, path: &Path, old: u64, added: usize) -> Result<(), StorageError> {
        if self.target != path || self.first_row != old || self.row_count != added as u64 {
            return Err(StorageError::FormatMismatch(
                "encoded append table or row count changed".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn stage(
        &self,
        directory: &Path,
        sequence: u32,
        explicit_cells: bool,
    ) -> Result<bool, StorageError> {
        let Some(manager) = self.managers.get(&sequence) else {
            return Ok(false);
        };
        if explicit_cells {
            return Err(StorageError::FormatMismatch(format!(
                "encoded append includes explicit cells for {}",
                manager.column
            )));
        }
        for (input, index, length) in &manager.files {
            let destination = tsm_data_path(directory, sequence, *index);
            match fs::symlink_metadata(tsm_data_path(&self.target, sequence, *index)) {
                Ok(_) => {
                    return Err(StorageError::FormatMismatch(
                        "encoded append payload already exists".into(),
                    ));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                Err(error) => return Err(error.into()),
            }
            if fs::copy(input, destination)? < *length {
                return Err(StorageError::FormatMismatch(format!(
                    "truncated tiled payload {}",
                    input.display()
                )));
            }
        }
        write_tiled_header(
            &directory.join(format!("table.f{sequence}")),
            &manager.variant,
            &manager.header,
        )?;
        Ok(true)
    }
}

fn prepare_manager(
    (target, target_dat): (&Path, &TableDatContents),
    (donor, donor_dat): (&Path, &TableDatContents),
    column: &str,
    payload_names: &BTreeSet<OsString>,
) -> Result<Option<(u32, Manager)>, StorageError> {
    let Some((target_seq, target_desc)) = shape_column(target_dat, column) else {
        return Ok(None);
    };
    let Some((donor_seq, donor_desc)) = shape_column(donor_dat, column) else {
        return Ok(None);
    };
    if target_desc.data_type != donor_desc.data_type || target_desc.nrdim != donor_desc.nrdim {
        return Ok(None);
    }
    let (mut variant, mut header) =
        read_tiled_header(&target.join(format!("table.f{target_seq}")))?;
    let (donor_variant, donor_header) =
        read_tiled_header(&donor.join(format!("table.f{donor_seq}")))?;
    if header.nrrow != target_dat.nrrow
        || donor_header.nrrow != donor_dat.nrrow
        || header.col_data_types.len() != 1
        || !plain_hypercolumn(target_dat, &header, column)
        || !plain_hypercolumn(donor_dat, &donor_header, column)
    {
        return Ok(None);
    }
    let reserved = |index| {
        let path = tsm_data_path(target, target_seq, index);
        path.file_name()
            .is_some_and(|name| payload_names.contains(name))
    };
    let Some(files) = splice_shape_header(
        &mut variant,
        &mut header,
        &donor_variant,
        &donor_header,
        reserved,
    ) else {
        return Ok(None);
    };
    Ok(Some((
        target_seq,
        Manager {
            column: column.to_owned(),
            variant,
            header,
            files: files
                .into_iter()
                .map(|(old, new, len)| (tsm_data_path(donor, donor_seq, old), new, len))
                .collect(),
        },
    )))
}

fn shape_column<'a>(
    dat: &'a TableDatContents,
    name: &str,
) -> Option<(u32, &'a ColumnDescContents)> {
    let (column, binding) = dat
        .table_desc
        .columns
        .iter()
        .zip(&dat.column_set.columns)
        .find(|(column, _)| column.col_name == name)?;
    let sequence = binding.dm_seq_nr;
    if !column.is_array
        || !column.shape.is_empty()
        || dat
            .column_set
            .columns
            .iter()
            .filter(|entry| entry.dm_seq_nr == sequence)
            .count()
            != 1
        || !dat
            .column_set
            .data_managers
            .iter()
            .any(|manager| manager.seq_nr == sequence && manager.type_name == "TiledShapeStMan")
    {
        return None;
    }
    Some((sequence, column))
}

fn plain_hypercolumn(dat: &TableDatContents, header: &TiledStManHeader, column: &str) -> bool {
    let key = format!("Hypercolumn_{}", header.hypercolumn_name);
    match dat.table_desc.private_keywords.get(&key) {
        None => true,
        Some(Value::Record(record)) => {
            use casa_core::ArrayValue;
            matches!(record.get("data"), Some(Value::Array(ArrayValue::String(names)))
                if names.len() == 1 && names.iter().next().is_some_and(|name| name == column))
                && ["coord", "id"]
                    .into_iter()
                    .all(|key| match record.get(key) {
                        None => true,
                        Some(Value::Array(ArrayValue::String(names))) => {
                            names.iter().all(String::is_empty)
                        }
                        _ => false,
                    })
        }
        _ => false,
    }
}

fn check_row_map(
    header: &TiledStManHeader,
    count: u32,
    rows: &[u32],
    cubes: &[u32],
    positions: &[u32],
) -> Option<()> {
    let count = count as usize;
    let rows = rows.get(..count)?;
    let cubes = cubes.get(..count)?;
    let positions = positions.get(..count)?;
    if rows.windows(2).any(|pair| pair[0] >= pair[1])
        || rows
            .last()
            .is_some_and(|&end| u64::from(end) >= header.nrrow)
    {
        return None;
    }
    let mut first = 0;
    for ((&last, &cube), &position) in rows.iter().zip(cubes).zip(positions) {
        if cube != 0 {
            let length = *header.cubes.get(cube as usize)?.cube_shape.last()?;
            if position < last - first || position as usize >= length {
                return None;
            }
        }
        first = last.checked_add(1)?;
    }
    Some(())
}

fn splice_shape_header(
    target_variant: &mut TiledVariant,
    target: &mut TiledStManHeader,
    donor_variant: &TiledVariant,
    donor: &TiledStManHeader,
    reserved: impl Fn(u32) -> bool,
) -> Option<Vec<(u32, u32, u64)>> {
    let TiledVariant::Shape {
        nr_used_row_map: count,
        row_map: rows,
        cube_map: cubes,
        pos_map: positions,
        ..
    } = target_variant
    else {
        return None;
    };
    let TiledVariant::Shape {
        nr_used_row_map: donor_count,
        row_map: donor_rows,
        cube_map: donor_cubes,
        pos_map: donor_positions,
        ..
    } = donor_variant
    else {
        return None;
    };
    if target.big_endian != donor.big_endian
        || target.nrdim != donor.nrdim
        || target.col_data_types != donor.col_data_types
        || target.cubes.first()?.file_seq_nr != -1
        || donor.cubes.first()?.file_seq_nr != -1
    {
        return None;
    }

    let first_appended_row = u32::try_from(target.nrrow).ok()?;
    let total = target.nrrow.checked_add(donor.nrrow)?;
    u32::try_from(total).ok()?;

    check_row_map(target, *count, rows, cubes, positions)?;
    check_row_map(
        donor,
        *donor_count,
        donor_rows,
        donor_cubes,
        donor_positions,
    )?;
    let mut files = Vec::new();
    for (old_index, file) in donor.files.iter().enumerate() {
        let Some(file) = file else {
            continue;
        };
        if file.seq_nr as usize != old_index || file.length < 0 {
            return None;
        }
        let mut next = u32::try_from(target.files.len()).ok()?;
        // A materialized rewrite can leave unused payload files behind.
        // Leave those files alone and keep a vacant entry for each index.
        while reserved(next) {
            next = next.checked_add(1)?;
        }
        target.files.resize(next as usize, None);
        files.push((file.seq_nr, next, u64::try_from(file.length).ok()?));
        target.files.push(Some(TsmFileInfo {
            seq_nr: next,
            length: file.length,
        }));
    }

    let first_appended_cube = u32::try_from(target.cubes.len()).ok()?.checked_sub(1)?;
    for donor_cube in donor.cubes.iter().skip(1) {
        let mut cube = donor_cube.clone();
        let (_, new_file, _) = files
            .iter()
            .find(|(old, _, _)| i32::try_from(*old).ok() == Some(cube.file_seq_nr))?;
        cube.file_seq_nr = i32::try_from(*new_file).ok()?;
        target.cubes.push(cube);
    }

    let used = *count as usize;
    rows.truncate(used);
    cubes.truncate(used);
    positions.truncate(used);
    let used_donor = *donor_count as usize;
    // An omitted suffix in the first manager is undefined, including when
    // all original rows were undefined. Preserve it before a donor range.
    if used_donor > 0
        && let Some(last_target_row) = first_appended_row.checked_sub(1)
        && rows
            .last()
            .is_none_or(|last_map| *last_map < last_target_row)
    {
        rows.push(last_target_row);
        cubes.push(0);
        positions.push(0);
    }
    for ((&row, &cube), &position) in donor_rows
        .get(..used_donor)?
        .iter()
        .zip(donor_cubes.get(..used_donor)?)
        .zip(donor_positions.get(..used_donor)?)
    {
        rows.push(first_appended_row.checked_add(row)?);
        cubes.push(if cube == 0 {
            0
        } else {
            first_appended_cube.checked_add(cube)?
        });
        positions.push(position);
    }
    *count = u32::try_from(rows.len()).ok()?;
    target.nrrow = total;
    Some(files)
}

#[cfg(all(test, unix))]
mod tests {
    use casa_core::{ArrayValue, PrimitiveType, RecordValue};

    use super::*;
    use crate::{
        ColumnSchema, DataManagerKind, LockMode, LockOptions, Table, TableOptions, TableSchema,
        TableWriteLock,
    };

    fn table(path: &Path, entries: &[Option<Vec<f32>>]) {
        use super::super::{
            StreamedTiledShapeCubeLayout, StreamedTiledShapeValueType, StreamingTiledShapeWriter,
            install_streamed_tiled_shape_column,
        };
        let schema = TableSchema::new(vec![ColumnSchema::array_variable(
            "VALUES",
            PrimitiveType::Float32,
            Some(1),
        )])
        .unwrap();
        let options = TableOptions::new(path).with_data_manager(DataManagerKind::TiledShapeStMan);
        Table::with_schema(schema).save(options).unwrap();
        let mut table = Table::open(TableOptions::new(path)).unwrap();
        table
            .append_rows_in_place(entries.len(), &RecordValue::default())
            .unwrap();
        let base = path.join("values.tmp");
        let headers = path.join("table.f0");
        let (_, header) = read_tiled_header(&headers).unwrap();
        let mut counts = [1, 2].map(|size| {
            entries
                .iter()
                .flatten()
                .filter(|cell| cell.len() == size)
                .count()
        });
        counts[0] += entries.iter().filter(|entry| entry.is_none()).count();
        let layouts = counts
            .into_iter()
            .enumerate()
            .filter(|(_, rows)| *rows != 0)
            .map(|(axis, rows)| {
                StreamedTiledShapeCubeLayout::new(vec![axis + 1], rows, vec![axis + 1, 1])
            })
            .collect();
        let mut writer = StreamingTiledShapeWriter::create(
            &base,
            StreamedTiledShapeValueType::Float32,
            layouts,
            4096,
            header.big_endian,
        )
        .unwrap();
        for cell in entries {
            if let Some(values) = cell {
                writer.push_f32_row(&[values.len()], values).unwrap();
            } else {
                writer.push_undefined_row().unwrap();
            }
        }
        fs::remove_file(headers).unwrap();
        install_streamed_tiled_shape_column(path, 0, "VALUES", writer.finish().unwrap()).unwrap();
    }

    #[test]
    fn encoded_shape_append_preserves_undefined_ranges_and_rolls_back() {
        let scratch = tempfile::tempdir().unwrap();
        let target = scratch.path().join("target");
        let donor = scratch.path().join("donor");
        table(&target, &[Some(vec![7.0]), None]);
        table(&donor, &[None, Some(vec![1.0, 2.0]), Some(vec![3.0]), None]);
        let mut guard = TableWriteLock::acquire_to_reread(&target, 1).unwrap();
        let reader = Table::open_with_lock_attempts(
            TableOptions::new(&donor),
            LockOptions::new(LockMode::PermanentLocking).read_only(true),
            1,
        )
        .unwrap();
        let writer = Table::open(TableOptions::new(&target)).unwrap();
        let unused_payload = tsm_data_path(&target, 0, 1);
        fs::write(&unused_payload, b"unused prior payload").unwrap();
        let encoded = writer
            .plan_encoded_array_append(&reader, &["VALUES"])
            .unwrap();
        assert_eq!(encoded.columns().collect::<Vec<_>>(), ["VALUES"]);
        let existing_data = tsm_data_path(&target, 0, 0);
        let existing_bytes = fs::read(&existing_data).unwrap();
        let records = (0..4).map(|_| RecordValue::default()).collect::<Vec<_>>();
        let mut append = writer
            .prepare_record_append_with_options(
                &records,
                crate::RecordAppendOptions {
                    encoded_arrays: Some(&encoded),
                    ..Default::default()
                },
                &mut guard,
            )
            .unwrap();
        append.publish().unwrap();
        append.synchronize().unwrap();
        let reopened = Table::open(TableOptions::new(&target)).unwrap();
        let rows = reopened
            .column_accessor("VALUES")
            .unwrap()
            .array_cells_owned_uncached(&[0, 1, 2, 3, 4, 5])
            .unwrap();
        assert_eq!(
            rows,
            [
                Some(vec![7.0]),
                None,
                None,
                Some(vec![1.0, 2.0]),
                Some(vec![3.0]),
                None
            ]
            .map(|cell| cell.map(ArrayValue::from_f32_vec))
        );
        assert_eq!(fs::read(&existing_data).unwrap(), existing_bytes);
        append.restore().unwrap();
        assert_eq!(
            Table::open(TableOptions::new(&target)).unwrap().row_count(),
            2
        );
        assert_eq!(fs::read(&existing_data).unwrap(), existing_bytes);
        assert_eq!(fs::read(unused_payload).unwrap(), b"unused prior payload");
        assert!(!tsm_data_path(&target, 0, 2).exists());
    }
}
