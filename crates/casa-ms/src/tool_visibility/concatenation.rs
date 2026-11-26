// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reuse open physical member tables for visibility reads in logical row order.

use std::collections::{HashMap, hash_map::Entry};

use casa_tables::{RequiredScalarColumnValues, Table};
use casa_types::ArrayValue;
use ndarray::ArrayD;

use super::{input, shaped};
use crate::MsResult;

pub(crate) struct MemberRows<'a> {
    table: &'a Table,
    rows: Vec<usize>,
    output_positions: Vec<usize>,
}

pub(crate) fn member_rows<'a>(
    tables: &'a [Table],
    total_rows: usize,
    rows: &[usize],
) -> Vec<MemberRows<'a>> {
    if tables.is_empty()
        || rows.is_empty()
        || tables.iter().map(Table::row_count).sum::<usize>() != total_rows
    {
        return Vec::new();
    }
    let mut permutation = Vec::new();
    let ordered_rows;
    let mut tail = rows;
    if !rows.is_sorted() {
        permutation.extend(0..rows.len());
        permutation.sort_unstable_by_key(|&slot| rows[slot]);
        ordered_rows = permutation
            .iter()
            .map(|&slot| rows[slot])
            .collect::<Vec<_>>();
        tail = &ordered_rows;
    }
    if tail.last().is_some_and(|&row| row >= total_rows) {
        return Vec::new();
    }
    let mut offset = 0;
    let mut positions = permutation.as_slice();
    let mut selected = Vec::new();
    for table in tables {
        if tail.is_empty() {
            break;
        }
        let end = offset + table.row_count();
        let count = tail.partition_point(|&row| row < end);
        let (local, remaining) = tail.split_at(count);
        if !local.is_empty() {
            let output_positions = if positions.is_empty() {
                Vec::new()
            } else {
                let (assigned, next) = positions.split_at(count);
                positions = next;
                assigned.to_vec()
            };
            selected.push(MemberRows {
                table,
                rows: local.iter().map(|row| row - offset).collect(),
                output_positions,
            });
        }
        tail = remaining;
        offset = end;
    }
    selected
}

fn restore_order<T: Copy>(values: Vec<T>, members: &[MemberRows<'_>], elements: usize) -> Vec<T> {
    if values.is_empty() || members[0].output_positions.is_empty() {
        return values;
    }
    let mut restored = vec![values[0]; values.len()];
    let positions = members
        .iter()
        .flat_map(|part| part.output_positions.iter().copied());
    for (source, destination) in values.chunks_exact(elements).zip(positions) {
        let start = destination * elements;
        restored[start..start + elements].copy_from_slice(source);
    }
    restored
}

pub(crate) fn concatenate_scalars(
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
    if members
        .first()
        .is_some_and(|member| !member.output_positions.is_empty())
    {
        for column in values.values_mut() {
            match column {
                Bool(values) => *values = restore_order(std::mem::take(values), members, 1),
                Int32(values) => *values = restore_order(std::mem::take(values), members, 1),
                Float32(values) => *values = restore_order(std::mem::take(values), members, 1),
                Float64(values) => *values = restore_order(std::mem::take(values), members, 1),
            }
        }
    }
    Ok(values)
}

pub(super) fn concatenate_arrays(
    members: &[MemberRows<'_>],
    read: impl Fn(&Table, &[usize]) -> MsResult<ArrayValue>,
) -> MsResult<ArrayValue> {
    let (initial_member, remaining) = members.split_first().expect("nonempty selected members");
    let first = read(initial_member.table, &initial_member.rows)?;
    if remaining.is_empty() && initial_member.output_positions.is_empty() {
        return Ok(first);
    }
    let total_rows = remaining.iter().map(|part| part.rows.len()).sum::<usize>();
    macro_rules! concatenate {
        ($variant:ident, $first:ident) => {{
            let mut shape = $first.shape().to_vec();
            let axis = shape.len() - 1;
            if shape[axis] != initial_member.rows.len() {
                return Err(input(
                    "concatenated array row count does not match its selection",
                ));
            }
            let initial = mem_order(&$first)?;
            let elements = shape[..axis].iter().product();
            let expected = (shape[axis] + total_rows)
                .checked_mul(elements)
                .ok_or_else(|| input("concatenated visibility array is too large"))?;
            let mut values = if initial_member.output_positions.is_empty() || initial.is_empty() {
                Vec::with_capacity(expected)
            } else {
                vec![initial[0]; expected]
            };
            append_cells(&mut values, initial, initial_member, elements);
            drop($first);
            for part in remaining {
                let ArrayValue::$variant(block) = read(part.table, &part.rows)? else {
                    return Err(input("concatenated array columns have different types"));
                };
                if block.ndim() != shape.len()
                    || block.shape()[..axis] != shape[..axis]
                    || block.shape()[axis] != part.rows.len()
                {
                    return Err(input(
                        "concatenated array columns have different cell shapes",
                    ));
                }
                shape[axis] += block.shape()[axis];
                append_cells(&mut values, mem_order(&block)?, part, elements);
            }
            Ok(ArrayValue::$variant(shaped(&shape, values)?))
        }};
    }
    match first {
        ArrayValue::Bool(values) => concatenate!(Bool, values),
        ArrayValue::Float32(values) => concatenate!(Float32, values),
        ArrayValue::Float64(values) => concatenate!(Float64, values),
        ArrayValue::Complex32(values) => concatenate!(Complex32, values),
        ArrayValue::Complex64(values) => concatenate!(Complex64, values),
        _ => Err(input("unsupported concatenated array column type")),
    }
}

fn append_cells<T: Copy>(
    output: &mut Vec<T>,
    cells: &[T],
    member: &MemberRows<'_>,
    elements: usize,
) {
    if member.output_positions.is_empty() {
        output.extend_from_slice(cells);
    } else if elements != 0 {
        for (cell, &position) in cells.chunks_exact(elements).zip(&member.output_positions) {
            let first = position * elements;
            output[first..first + elements].copy_from_slice(cell);
        }
    }
}

fn mem_order<T>(block: &ArrayD<T>) -> MsResult<&[T]> {
    block
        .as_slice_memory_order()
        .ok_or_else(|| input("visibility block is not contiguous"))
}
