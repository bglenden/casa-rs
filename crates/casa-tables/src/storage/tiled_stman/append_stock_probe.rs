use super::*;
use crate::storage::TableDatContents;
// Experiment for stock heterogeneous-shape MAIN attachment.
// Called from storage::tiled_stman so it uses the existing casacore parser,
// row-map representation and serializer.
fn splice_shape_header(
    target_variant: &mut TiledVariant,
    target: &mut TiledStManHeader,
    donor_variant: &TiledVariant,
    donor: &TiledStManHeader,
) -> Option<Vec<(u32, u32)>> {
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

    let mut files = Vec::new();
    for (old_index, file) in donor.files.iter().enumerate() {
        let Some(file) = file else {
            continue;
        };
        if file.seq_nr as usize != old_index || file.length < 0 {
            return None;
        }
        let next = u32::try_from(target.files.len()).ok()?;
        files.push((file.seq_nr, next));
        target.files.push(Some(TsmFileInfo {
            seq_nr: next,
            length: file.length,
        }));
    }

    let first_appended_cube = u32::try_from(target.cubes.len()).ok()?.checked_sub(1)?;
    for donor_cube in donor.cubes.iter().skip(1) {
        let mut cube = donor_cube.clone();
        let (_, new_file) = files
            .iter()
            .find(|(old, _)| i32::try_from(*old).ok() == Some(cube.file_seq_nr))?;
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

#[test]
fn stock_shape_headers_can_attach_existing_encoded_payload() {
    use std::{fs, time::Instant};
    let a = PathBuf::from(std::env::var_os("CONCAT_STOCK_FIRST").unwrap());
    let b = PathBuf::from(std::env::var_os("CONCAT_STOCK_DONOR").unwrap());
    let a_dat = crate::storage::table_control::read_table_dat(&a.join("table.dat")).unwrap();
    let b_dat = crate::storage::table_control::read_table_dat(&b.join("table.dat")).unwrap();
    let root = tempfile::tempdir().unwrap();

    for column in ["DATA", "FLAG", "WEIGHT", "SIGMA", "WEIGHT_SPECTRUM"] {
        let find = |dat: &TableDatContents| {
            dat.table_desc
                .columns
                .iter()
                .position(|col| col.col_name == column)
                .unwrap()
        };
        let a_col = find(&a_dat);
        let b_col = find(&b_dat);
        let a_entry = &a_dat.column_set.columns[a_col];
        let b_entry = &b_dat.column_set.columns[b_col];
        let a_seq = a_entry.dm_seq_nr;
        let b_seq = b_entry.dm_seq_nr;
        let a_dm = a_dat
            .column_set
            .data_managers
            .iter()
            .find(|mgr| mgr.seq_nr == a_seq)
            .unwrap();
        let b_dm = b_dat
            .column_set
            .data_managers
            .iter()
            .find(|mgr| mgr.seq_nr == b_seq)
            .unwrap();
        let (mut av, mut ah) = read_tiled_header(&a.join(format!("table.f{a_seq}"))).unwrap();
        let (bv, bh) = read_tiled_header(&b.join(format!("table.f{b_seq}"))).unwrap();
        for file in ah.files.iter().flatten() {
            fs::copy(
                tsm_data_path(&a, a_seq, file.seq_nr),
                tsm_data_path(root.path(), a_seq, file.seq_nr),
            )
            .unwrap();
        }
        let start = Instant::now();
        let ids = splice_shape_header(&mut av, &mut ah, &bv, &bh).unwrap_or_else(|| {
            panic!(
                "{column}: compatible manager metadata; target endian={} donor endian={}",
                ah.big_endian, bh.big_endian
            )
        });
        let mut bytes = 0;
        for (donor_id, target_id) in ids {
            bytes += fs::copy(
                tsm_data_path(&b, b_seq, donor_id),
                tsm_data_path(root.path(), a_seq, target_id),
            )
            .unwrap();
        }
        write_tiled_header(&root.path().join(format!("table.f{a_seq}")), &av, &ah).unwrap();
        let elapsed = start.elapsed().as_secs_f64() * 1000.;

        let expected_a = load_tiled_column_rows(
            &a,
            a_dm,
            &a_dat.table_desc,
            &[(a_col, a_entry)],
            a_col,
            &[0, a_dat.nrrow as usize - 1],
        )
        .unwrap();
        let expected_b = load_tiled_column_rows(
            &b,
            b_dm,
            &b_dat.table_desc,
            &[(b_col, b_entry)],
            b_col,
            &[0, b_dat.nrrow as usize - 1],
        )
        .unwrap();
        let actual = load_tiled_column_rows(
            root.path(),
            a_dm,
            &a_dat.table_desc,
            &[(a_col, a_entry)],
            a_col,
            &[
                0,
                a_dat.nrrow as usize - 1,
                a_dat.nrrow as usize,
                a_dat.nrrow as usize + b_dat.nrrow as usize - 1,
            ],
        )
        .unwrap();
        assert_eq!(&actual[..2], expected_a.as_slice(), "{column} old boundary");
        assert_eq!(
            &actual[2..],
            expected_b.as_slice(),
            "{column} donor boundary"
        );
        println!(
            "{column}: retained old payload, copied {bytes} encoded bytes, header + copy {elapsed:.3} ms"
        );
    }
}

#[test]
fn stock_main_can_append_without_repacking_its_shape_managers() {
    use std::{fs, time::Instant};

    let a = PathBuf::from(std::env::var_os("CONCAT_STOCK_FIRST").unwrap());
    let b = PathBuf::from(std::env::var_os("CONCAT_STOCK_DONOR").unwrap());
    let result = PathBuf::from(std::env::var_os("CONCAT_STOCK_PROBE_OUTPUT").unwrap());
    let grafts = tempfile::tempdir_in(result.parent().unwrap()).unwrap();
    let a_dat = crate::storage::table_control::read_table_dat(&a.join("table.dat")).unwrap();
    let b_dat = crate::storage::table_control::read_table_dat(&b.join("table.dat")).unwrap();
    crate::publication::copy_locked_table_tree(&a, &result).unwrap();
    let starts = Instant::now();

    for (a_group, b_group) in [(2, 2), (3, 3), (4, 4), (5, 5), (7, 7), (8, 8)] {
        let (mut av, mut ah) = read_tiled_header(&a.join(format!("table.f{a_group}"))).unwrap();
        let (bv, bh) = read_tiled_header(&b.join(format!("table.f{b_group}"))).unwrap();
        let ids = splice_shape_header(&mut av, &mut ah, &bv, &bh).unwrap();
        for (source, destination) in ids {
            fs::copy(
                tsm_data_path(&b, b_group, source),
                tsm_data_path(grafts.path(), a_group, destination),
            )
            .unwrap();
        }
        write_tiled_header(&grafts.path().join(format!("table.f{a_group}")), &av, &ah).unwrap();
    }
    let graft_time = starts.elapsed();

    let scale = Instant::now();
    let names = b_dat
        .table_desc
        .columns
        .iter()
        .filter(|column| !column.is_array && !column.is_record())
        .map(|column| column.col_name.as_str())
        .collect::<Vec<_>>();
    let scalar = crate::Table::read_scalar_columns_from_disk(&b, &names).unwrap();
    let scalar_time = scale.elapsed();
    let donor = crate::Table::open(crate::TableOptions::new(&b)).unwrap();
    let ids = (0..b_dat.nrrow as usize).collect::<Vec<_>>();
    let uvw = donor.get_array_cells_owned("UVW", &ids).unwrap();
    let uvw_time = scale.elapsed() - scalar_time;
    let rows = uvw
        .into_iter()
        .enumerate()
        .map(|(row, uvw)| {
            let mut fields = Vec::with_capacity(scalar.len() + 1);
            for (name, values) in &scalar {
                if let Some(value) = &values[row] {
                    fields.push(casa_types::RecordField::new(
                        name,
                        Value::Scalar(value.clone()),
                    ));
                }
            }
            if let Some(uvw) = uvw {
                fields.push(casa_types::RecordField::new("UVW", Value::Array(uvw)));
            }
            RecordValue::new(fields)
        })
        .collect::<Vec<_>>();
    let read_time = scale.elapsed();
    println!(
        "donor scalar {:.2} ms, UVW {:.2} ms, record allocation {:.2} ms",
        scalar_time.as_secs_f64() * 1000.,
        uvw_time.as_secs_f64() * 1000.,
        (read_time - scalar_time - uvw_time).as_secs_f64() * 1000.
    );

    let apply = Instant::now();
    let mut held = crate::TableWriteLock::acquire_to_reread(&result, 1).unwrap();
    held.record_write();
    let table = crate::Table::open(crate::TableOptions::new(&result)).unwrap();
    table.append_records_and_close(&rows).unwrap();
    for file in fs::read_dir(grafts.path()).unwrap() {
        let file = file.unwrap();
        fs::rename(file.path(), result.join(file.file_name())).unwrap();
    }
    held.release().unwrap();
    let apply_time = apply.elapsed();
    println!(
        "raw stock MAIN {:.2} ms, encoded graft {:.2}, donor scalar/UVW reads {:.2}, remainder append {:.2}",
        starts.elapsed().as_secs_f64() * 1000.,
        graft_time.as_secs_f64() * 1000.,
        read_time.as_secs_f64() * 1000.,
        apply_time.as_secs_f64() * 1000.,
    );

    let output = crate::Table::open(crate::TableOptions::new(&result)).unwrap();
    assert_eq!(output.row_count(), (a_dat.nrrow + b_dat.nrrow) as usize);
    for column in ["DATA", "FLAG", "WEIGHT", "SIGMA", "UVW", "WEIGHT_SPECTRUM"] {
        let before = crate::Table::open(crate::TableOptions::new(&a)).unwrap();
        assert_eq!(
            output
                .get_array_cells_owned(column, &[a_dat.nrrow as usize - 1])
                .unwrap(),
            before
                .get_array_cells_owned(column, &[a_dat.nrrow as usize - 1])
                .unwrap(),
            "{column} old boundary",
        );
        assert_eq!(
            output
                .get_array_cells_owned(column, &[a_dat.nrrow as usize])
                .unwrap(),
            donor.get_array_cells_owned(column, &[0]).unwrap(),
            "{column} donor boundary",
        );
    }
}
