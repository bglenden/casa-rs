// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Copy data from a cube array into a tile (reverse of `copy_tile_to_cube`).
pub(super) fn copy_cube_to_tile(
    cube_data: &[u8],
    cube_shape: &[usize],
    tile_data: &mut [u8],
    tile_shape: &[usize],
    cube_start: &[usize],
    actual_extent: &[usize],
    elem_size: usize,
) {
    let ndim = tile_shape.len();
    if ndim == 0 {
        return;
    }

    let inner_bytes = actual_extent[0] * elem_size;

    if ndim == 1 {
        let src_off = cube_start[0] * elem_size;
        let dst_off = 0;
        tile_data[dst_off..dst_off + inner_bytes]
            .copy_from_slice(&cube_data[src_off..src_off + inner_bytes]);
        return;
    }
    if ndim == 2 && actual_extent[0] == cube_shape[0] && actual_extent[0] == tile_shape[0] {
        let start = cube_start[1] * inner_bytes;
        let len = actual_extent[1] * inner_bytes;
        tile_data[..len].copy_from_slice(&cube_data[start..start + len]);
        return;
    }
    // A single FLAG_CATEGORY plane shares the contiguous correlation and
    // channel axes of its source cell. Copy the plane as one run, including
    // when the channel tile covers only part of that axis.
    if ndim == 3
        && actual_extent[2] == 1
        && actual_extent[0] == cube_shape[0]
        && actual_extent[0] == tile_shape[0]
    {
        let start = (cube_start[2] * cube_shape[1] + cube_start[1]) * inner_bytes;
        let len = actual_extent[1] * inner_bytes;
        tile_data[..len].copy_from_slice(&cube_data[start..start + len]);
        return;
    }

    let outer_dims = &actual_extent[1..];
    let outer_total: usize = outer_dims.iter().product();
    let mut outer_pos = vec![0usize; outer_dims.len()];

    for _ in 0..outer_total {
        let mut tile_off = 0;
        let mut stride = tile_shape[0];
        for (d, &p) in outer_pos.iter().enumerate() {
            tile_off += p * stride;
            stride *= tile_shape[d + 1];
        }

        let mut cube_off = cube_start[0];
        stride = cube_shape[0];
        for (d, &p) in outer_pos.iter().enumerate() {
            cube_off += (cube_start[d + 1] + p) * stride;
            stride *= cube_shape[d + 1];
        }

        let src_start = cube_off * elem_size;
        let dst_start = tile_off * elem_size;
        tile_data[dst_start..dst_start + inner_bytes]
            .copy_from_slice(&cube_data[src_start..src_start + inner_bytes]);

        increment_position(&mut outer_pos, outer_dims);
    }
}

pub(super) fn fill_cube_tile_region(
    tile_data: &mut [u8],
    tile_shape: &[usize],
    actual_extent: &[usize],
    elem_size: usize,
    value: u8,
) {
    let ndim = tile_shape.len();
    if ndim == 0 {
        return;
    }

    let inner_bytes = actual_extent[0] * elem_size;

    if ndim == 1 {
        tile_data[..inner_bytes].fill(value);
        return;
    }

    let outer_dims = &actual_extent[1..];
    let outer_total: usize = outer_dims.iter().product();
    let mut outer_pos = vec![0usize; outer_dims.len()];

    for _ in 0..outer_total {
        let mut tile_off = 0;
        let mut stride = tile_shape[0];
        for (d, &p) in outer_pos.iter().enumerate() {
            tile_off += p * stride;
            stride *= tile_shape[d + 1];
        }

        let dst_start = tile_off * elem_size;
        tile_data[dst_start..dst_start + inner_bytes].fill(value);

        increment_position(&mut outer_pos, outer_dims);
    }
}

pub(super) fn increment_position(pos: &mut [usize], dims: &[usize]) {
    for (axis, dim) in pos.iter_mut().zip(dims.iter()) {
        *axis += 1;
        if *axis < *dim {
            break;
        }
        *axis = 0;
    }
}

/// Get the shape of an ArrayValue as a Vec<usize>.
pub(super) fn array_shape(av: &ArrayValue) -> Vec<usize> {
    match av {
        ArrayValue::Bool(a) => a.shape().to_vec(),
        ArrayValue::UInt8(a) => a.shape().to_vec(),
        ArrayValue::Int16(a) => a.shape().to_vec(),
        ArrayValue::UInt16(a) => a.shape().to_vec(),
        ArrayValue::Int32(a) => a.shape().to_vec(),
        ArrayValue::UInt32(a) => a.shape().to_vec(),
        ArrayValue::Float32(a) => a.shape().to_vec(),
        ArrayValue::Float64(a) => a.shape().to_vec(),
        ArrayValue::Int64(a) => a.shape().to_vec(),
        ArrayValue::Complex32(a) => a.shape().to_vec(),
        ArrayValue::Complex64(a) => a.shape().to_vec(),
        ArrayValue::String(a) => a.shape().to_vec(),
    }
}

pub(super) fn array_shape_equals(av: &ArrayValue, shape: &[usize]) -> bool {
    match av {
        ArrayValue::Bool(a) => a.shape() == shape,
        ArrayValue::UInt8(a) => a.shape() == shape,
        ArrayValue::Int16(a) => a.shape() == shape,
        ArrayValue::UInt16(a) => a.shape() == shape,
        ArrayValue::Int32(a) => a.shape() == shape,
        ArrayValue::UInt32(a) => a.shape() == shape,
        ArrayValue::Float32(a) => a.shape() == shape,
        ArrayValue::Float64(a) => a.shape() == shape,
        ArrayValue::Int64(a) => a.shape() == shape,
        ArrayValue::Complex32(a) => a.shape() == shape,
        ArrayValue::Complex64(a) => a.shape() == shape,
        ArrayValue::String(a) => a.shape() == shape,
    }
}

pub(super) fn same_shape_group(values: &[Option<&Value>]) -> Option<Vec<(Vec<usize>, Vec<usize>)>> {
    let (first_row, first_value) = values
        .iter()
        .enumerate()
        .find_map(|(row, value)| value.map(|value| (row, value)))?;
    let first_shape = if let Value::Array(av) = first_value {
        array_shape(av)
    } else {
        Vec::new()
    };
    let mut group_rows = Vec::with_capacity(values.len());
    group_rows.push(first_row);
    for (row, value) in values.iter().enumerate().skip(first_row + 1) {
        let Some(value) = *value else {
            continue;
        };
        let matches = if let Value::Array(av) = value {
            array_shape_equals(av, &first_shape)
        } else {
            first_shape.is_empty()
        };
        if !matches {
            return None;
        }
        group_rows.push(row);
    }
    Some(vec![(first_shape, group_rows)])
}
