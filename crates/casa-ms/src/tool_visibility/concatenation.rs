// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reuse open physical member tables for visibility reads in logical row order.

use std::collections::{HashMap, hash_map::Entry};

use casa_tables::{RequiredScalarColumnValues, Table};
use casa_types::ArrayValue;
use ndarray::ArrayD;

use super::{input, shaped};
use crate::MsResult;

pub(super) struct MemberRows<'a> {
    table: &'a Table,
    rows: Vec<usize>,
}

pub(super) fn member_rows<'a>(
    tables: &'a [Table],
    total_rows: usize,
    rows: &[usize],
) -> Vec<MemberRows<'a>> {
    if tables.is_empty()
        || rows.is_empty()
        || !rows.is_sorted()
        || rows.last().is_some_and(|&row| row >= total_rows)
        || tables.iter().map(Table::row_count).sum::<usize>() != total_rows
    {
        return Vec::new();
    }
    let mut offset = 0;
    let mut tail = rows;
    let mut selected = Vec::new();
    for table in tables {
        if tail.is_empty() {
            break;
        }
        let end = offset + table.row_count();
        let count = tail.partition_point(|&row| row < end);
        let (local, remaining) = tail.split_at(count);
        if !local.is_empty() {
            selected.push(MemberRows {
                table,
                rows: local.iter().map(|row| row - offset).collect(),
            });
        }
        tail = remaining;
        offset = end;
    }
    selected
}

pub(super) fn concatenate_scalars(
    members: &[MemberRows<'_>],
    columns: &[&str],
) -> MsResult<HashMap<String, RequiredScalarColumnValues>> {
    use RequiredScalarColumnValues::{Bool, Float32, Float64, Int32};

    let mut values = HashMap::with_capacity(columns.len());
    for part in members {
        for (name, loaded) in part
            .table
            .required_scalar_columns_owned_for_rows(columns, &part.rows)?
        {
            match values.entry(name) {
                Entry::Vacant(entry) => {
                    entry.insert(loaded);
                }
                Entry::Occupied(mut entry) => match (entry.get_mut(), loaded) {
                    (Bool(result), Bool(part)) => result.extend(part),
                    (Int32(result), Int32(part)) => result.extend(part),
                    (Float32(result), Float32(part)) => result.extend(part),
                    (Float64(result), Float64(part)) => result.extend(part),
                    _ => return Err(input("concatenated scalar columns have different types")),
                },
            }
        }
    }
    Ok(values)
}

pub(super) fn concatenate_arrays(
    members: &[MemberRows<'_>],
    read: impl Fn(&Table, &[usize]) -> MsResult<ArrayValue>,
) -> MsResult<ArrayValue> {
    let (first, members) = members.split_first().expect("nonempty selected members");
    let first = read(first.table, &first.rows)?;
    if members.is_empty() {
        return Ok(first);
    }
    let total_rows = members.iter().map(|part| part.rows.len()).sum::<usize>();
    macro_rules! concatenate {
        ($variant:ident, $first:ident) => {{
            let mut shape = $first.shape().to_vec();
            let axis = shape.len() - 1;
            let initial = mem_order(&$first)?;
            let expected = (shape[axis] + total_rows)
                .checked_mul(shape[..axis].iter().product())
                .ok_or_else(|| input("concatenated visibility array is too large"))?;
            let mut values = Vec::with_capacity(expected);
            values.extend_from_slice(initial);
            for part in members {
                let ArrayValue::$variant(block) = read(part.table, &part.rows)? else {
                    return Err(input("concatenated array columns have different types"));
                };
                if block.ndim() != shape.len() || block.shape()[..axis] != shape[..axis] {
                    return Err(input(
                        "concatenated array columns have different cell shapes",
                    ));
                }
                shape[axis] += block.shape()[axis];
                values.extend_from_slice(mem_order(&block)?);
            }
            Ok(ArrayValue::$variant(shaped(&shape, values)?))
        }};
    }
    match first {
        ArrayValue::Bool(values) => concatenate!(Bool, values),
        ArrayValue::Float32(values) => concatenate!(Float32, values),
        ArrayValue::Float64(values) => concatenate!(Float64, values),
        ArrayValue::Complex32(values) => concatenate!(Complex32, values),
        _ => Err(input("unsupported concatenated array column type")),
    }
}

fn mem_order<T>(block: &ArrayD<T>) -> MsResult<&[T]> {
    block
        .as_slice_memory_order()
        .ok_or_else(|| input("visibility block is not contiguous"))
}
