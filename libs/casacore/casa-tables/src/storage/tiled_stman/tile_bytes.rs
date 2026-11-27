// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Returns the unpacked element size used while reconstructing typed tile data.
pub(super) fn tile_element_size(dt: CasacoreDataType) -> usize {
    match dt {
        CasacoreDataType::TpBool | CasacoreDataType::TpUChar | CasacoreDataType::TpChar => 1,
        CasacoreDataType::TpShort | CasacoreDataType::TpUShort => 2,
        CasacoreDataType::TpInt | CasacoreDataType::TpUInt | CasacoreDataType::TpFloat => 4,
        CasacoreDataType::TpDouble | CasacoreDataType::TpInt64 | CasacoreDataType::TpComplex => 8,
        CasacoreDataType::TpDComplex => 16,
        _ => 0,
    }
}

/// Returns the number of on-disk bytes a column occupies within one tile.
pub(super) fn tile_storage_bytes(dt: CasacoreDataType, nrpixels: usize) -> usize {
    match dt {
        CasacoreDataType::TpBool => nrpixels.div_ceil(8),
        _ => nrpixels * tile_element_size(dt),
    }
}

/// Compute tile layout: per-column offsets within a tile and total bucket size.
///
/// Lays the columns out one after another in the order given. Every caller
/// passes casacore's tile order (`tile_columns`, `TiledStMan::setup`'s
/// descending-size sort) or a single column's type.
pub(super) fn compute_tile_layout(
    col_data_types: &[CasacoreDataType],
    tile_shape: &[usize],
) -> (usize, Vec<usize>) {
    let nrpixels: usize = tile_shape.iter().product();

    let mut offsets = vec![0usize; col_data_types.len()];
    let mut offset = 0usize;
    for (col, &dt) in col_data_types.iter().enumerate() {
        offsets[col] = offset;
        offset += tile_storage_bytes(dt, nrpixels);
    }

    (offset, offsets)
}

pub(super) fn read_tile_storage(src: &[u8], dt: CasacoreDataType, nrpixels: usize) -> Vec<u8> {
    match dt {
        CasacoreDataType::TpBool => read_bool_bits(src, 0, nrpixels)
            .into_iter()
            .map(|value| if value { 1 } else { 0 })
            .collect(),
        _ => src.to_vec(),
    }
}

pub(super) fn write_tile_storage(
    dst: &mut [u8],
    dt: CasacoreDataType,
    unpacked: &[u8],
    nrpixels: usize,
) {
    match dt {
        CasacoreDataType::TpBool => write_bool_bits_from_bytes(dst, 0, &unpacked[..nrpixels]),
        _ => dst.copy_from_slice(unpacked),
    }
}

pub(super) fn write_bool_bits_fill(dst: &mut [u8], nrpixels: usize) {
    let full_bytes = nrpixels / 8;
    let remaining_bits = nrpixels % 8;
    dst[..full_bytes].fill(0xff);
    if remaining_bits > 0 {
        dst[full_bytes] = (1u8 << remaining_bits) - 1;
    }
}

/// Convert a linear index to an N-dimensional position (column-major order).
pub(super) fn linear_to_nd(mut linear: usize, dims: &[usize]) -> Vec<usize> {
    let mut pos = Vec::with_capacity(dims.len());
    for &d in dims {
        pos.push(linear % d);
        linear /= d;
    }
    pos
}

/// Convert an N-dimensional position to a linear index (column-major order).
pub(super) fn nd_to_linear(pos: &[usize], dims: &[usize]) -> usize {
    let mut linear = 0;
    let mut stride = 1;
    for i in 0..dims.len() {
        linear += pos[i] * stride;
        stride *= dims[i];
    }
    linear
}

/// Copy tile data into the correct position in a reconstructed cube.
///
/// Both tile and cube use column-major (Fortran) order. Handles edge tiles
/// that extend beyond the cube boundary.
pub(super) fn copy_tile_to_cube(
    tile_data: &[u8],
    tile_shape: &[usize],
    cube_data: &mut [u8],
    cube_shape: &[usize],
    cube_start: &[usize],
    actual_extent: &[usize],
    elem_size: usize,
) {
    let ndim = tile_shape.len();
    if ndim == 0 {
        return;
    }

    // Optimize: copy contiguous runs along the innermost dimension.
    let inner_bytes = actual_extent[0] * elem_size;

    if ndim == 1 {
        let src_off = 0;
        let dst_off = cube_start[0] * elem_size;
        cube_data[dst_off..dst_off + inner_bytes]
            .copy_from_slice(&tile_data[src_off..src_off + inner_bytes]);
        return;
    }

    // Iterate over all outer-dimension positions.
    let outer_dims = &actual_extent[1..];
    let outer_total: usize = outer_dims.iter().product();
    let mut outer_pos = vec![0usize; outer_dims.len()];

    for _ in 0..outer_total {
        // Compute tile linear offset for this outer position.
        let mut tile_off = 0;
        let mut stride = tile_shape[0];
        for (d, &p) in outer_pos.iter().enumerate() {
            tile_off += p * stride;
            stride *= tile_shape[d + 1];
        }

        // Compute cube linear offset.
        let mut cube_off = cube_start[0];
        stride = cube_shape[0];
        for (d, &p) in outer_pos.iter().enumerate() {
            cube_off += (cube_start[d + 1] + p) * stride;
            stride *= cube_shape[d + 1];
        }

        let src_start = tile_off * elem_size;
        let dst_start = cube_off * elem_size;
        cube_data[dst_start..dst_start + inner_bytes]
            .copy_from_slice(&tile_data[src_start..src_start + inner_bytes]);

        increment_position(&mut outer_pos, outer_dims);
    }
}

/// Reconstruct a single column's full cube data from tiled storage.
///
/// Returns raw bytes in column-major order, in the on-disk byte order.
#[allow(clippy::too_many_arguments)]
pub(super) fn reconstruct_cube_column(
    file_data: &[u8],
    file_offset: usize,
    cube_shape: &[usize],
    tile_shape: &[usize],
    col_offset_in_tile: usize,
    bucket_size: usize,
    dt: CasacoreDataType,
    elem_size: usize,
) -> Result<Vec<u8>, StorageError> {
    let ndim = cube_shape.len();
    let nrpixels: usize = tile_shape.iter().product();
    let col_bytes_per_tile = tile_storage_bytes(dt, nrpixels);

    let tiles_per_dim: Vec<usize> = (0..ndim)
        .map(|i| cube_shape[i].div_ceil(tile_shape[i]))
        .collect();
    let nr_tiles: usize = tiles_per_dim.iter().product();

    let cube_nelem: usize = cube_shape.iter().product();
    let mut result = vec![0u8; cube_nelem * elem_size];

    for tile_idx in 0..nr_tiles {
        let tile_pos = linear_to_nd(tile_idx, &tiles_per_dim);

        let cube_start: Vec<usize> = (0..ndim).map(|d| tile_pos[d] * tile_shape[d]).collect();
        let actual_extent: Vec<usize> = (0..ndim)
            .map(|d| std::cmp::min(tile_shape[d], cube_shape[d] - cube_start[d]))
            .collect();

        let tile_data_start = file_offset + tile_idx * bucket_size + col_offset_in_tile;
        let tile_data_end = tile_data_start + col_bytes_per_tile;
        if tile_data_end > file_data.len() {
            return Err(StorageError::FormatMismatch(format!(
                "tile data out of bounds: need {tile_data_end} bytes but file has {}",
                file_data.len()
            )));
        }
        let tile_bytes =
            read_tile_storage(&file_data[tile_data_start..tile_data_end], dt, nrpixels);
        copy_tile_to_cube(
            &tile_bytes,
            tile_shape,
            &mut result,
            cube_shape,
            &cube_start,
            &actual_extent,
            elem_size,
        );
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Typed value extraction from raw bytes
// ---------------------------------------------------------------------------

/// Decode raw bytes (in the table's byte order) into a `Value::Array`.
pub(super) fn decode_array_value(
    raw: &[u8],
    shape: &[usize],
    dt: CasacoreDataType,
    big_endian: bool,
) -> Result<Value, StorageError> {
    let nelem: usize = shape.iter().product();
    let av = match dt {
        CasacoreDataType::TpBool => {
            let vals: Vec<bool> = raw[..nelem].iter().map(|&b| b != 0).collect();
            ArrayValue::Bool(
                ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpUChar => {
            let vals: Vec<u8> = raw[..nelem].to_vec();
            ArrayValue::UInt8(
                ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpShort => {
            let vals = if big_endian {
                read_i16_slice_be(raw, nelem)
            } else {
                read_i16_slice_le(raw, nelem)
            };
            ArrayValue::Int16(
                ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpUShort => {
            let vals = if big_endian {
                read_u16_slice_be(raw, nelem)
            } else {
                read_u16_slice_le(raw, nelem)
            };
            ArrayValue::UInt16(
                ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpInt => {
            let vals = if big_endian {
                read_i32_slice_be(raw, nelem)
            } else {
                read_i32_slice_le(raw, nelem)
            };
            ArrayValue::Int32(
                ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpUInt => {
            let vals = if big_endian {
                read_u32_slice_be(raw, nelem)
            } else {
                read_u32_slice_le(raw, nelem)
            };
            ArrayValue::UInt32(
                ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpFloat => {
            let vals = if big_endian {
                read_f32_slice_be(raw, nelem)
            } else {
                read_f32_slice_le(raw, nelem)
            };
            ArrayValue::Float32(
                ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpDouble => {
            let vals = if big_endian {
                read_f64_slice_be(raw, nelem)
            } else {
                read_f64_slice_le(raw, nelem)
            };
            ArrayValue::Float64(
                ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpInt64 => {
            let vals = if big_endian {
                read_i64_slice_be(raw, nelem)
            } else {
                read_i64_slice_le(raw, nelem)
            };
            ArrayValue::Int64(
                ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpComplex => {
            // Complex = (float, float) pairs
            let vals = decode_complex32_values(raw, nelem, big_endian);
            ArrayValue::Complex32(
                ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpDComplex => {
            let vals = decode_complex64_values(raw, nelem, big_endian);
            ArrayValue::Complex64(
                ArrayD::from_shape_vec(IxDyn(shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        other => {
            return Err(StorageError::FormatMismatch(format!(
                "unsupported tiled array element type: {other:?}"
            )));
        }
    };
    Ok(Value::Array(av))
}

pub(super) fn decode_complex32_values(
    raw: &[u8],
    nelem: usize,
    big_endian: bool,
) -> Vec<Complex32> {
    let mut values = Vec::with_capacity(nelem);
    for chunk in raw[..nelem * 8].as_chunks::<8>().0 {
        values.push(decode_complex32_scalar(chunk, big_endian));
    }
    values
}

pub(super) fn decode_complex32_scalar(raw: &[u8], big_endian: bool) -> Complex32 {
    let re = if big_endian {
        f32::from_be_bytes(raw[0..4].try_into().expect("exact f32 bytes"))
    } else {
        f32::from_le_bytes(raw[0..4].try_into().expect("exact f32 bytes"))
    };
    let im = if big_endian {
        f32::from_be_bytes(raw[4..8].try_into().expect("exact f32 bytes"))
    } else {
        f32::from_le_bytes(raw[4..8].try_into().expect("exact f32 bytes"))
    };
    Complex32::new(re, im)
}

pub(super) fn decode_f32_scalar(raw: &[u8], big_endian: bool) -> f32 {
    if big_endian {
        f32::from_be_bytes(raw[0..4].try_into().expect("exact f32 bytes"))
    } else {
        f32::from_le_bytes(raw[0..4].try_into().expect("exact f32 bytes"))
    }
}

pub(super) fn decode_f64_scalar(raw: &[u8], big_endian: bool) -> f64 {
    if big_endian {
        f64::from_be_bytes(raw[0..8].try_into().expect("exact f64 bytes"))
    } else {
        f64::from_le_bytes(raw[0..8].try_into().expect("exact f64 bytes"))
    }
}

pub(super) fn decode_complex64_scalar(raw: &[u8], big_endian: bool) -> Complex64 {
    let re = if big_endian {
        f64::from_be_bytes(raw[0..8].try_into().expect("exact f64 bytes"))
    } else {
        f64::from_le_bytes(raw[0..8].try_into().expect("exact f64 bytes"))
    };
    let im = if big_endian {
        f64::from_be_bytes(raw[8..16].try_into().expect("exact f64 bytes"))
    } else {
        f64::from_le_bytes(raw[8..16].try_into().expect("exact f64 bytes"))
    };
    Complex64::new(re, im)
}

pub(super) fn decode_complex64_values(
    raw: &[u8],
    nelem: usize,
    big_endian: bool,
) -> Vec<Complex64> {
    let mut values = Vec::with_capacity(nelem);
    for chunk in raw[..nelem * 16].as_chunks::<16>().0 {
        values.push(decode_complex64_scalar(chunk, big_endian));
    }
    values
}

/// Encode a `Value::Array` into raw bytes in the specified byte order.
pub(super) fn encode_array_value(
    value: &Value,
    big_endian: bool,
) -> Result<(Vec<u8>, CasacoreDataType), StorageError> {
    let arr = match value {
        Value::Array(a) => a,
        _ => {
            return Err(StorageError::FormatMismatch(
                "expected array value for tiled column".to_string(),
            ));
        }
    };
    encode_tiled_array(arr, big_endian)
}

pub(super) fn encode_tiled_array(
    arr: &ArrayValue,
    big_endian: bool,
) -> Result<(Vec<u8>, CasacoreDataType), StorageError> {
    // Tile data is stored in Fortran (column-major) order. Most arrays read
    // from casacore tables already have that memory order, so use the raw
    // memory-order slice before falling back to a transposed contiguous copy.
    match arr {
        ArrayValue::Bool(a) => {
            let slice = array_memory_order_values(a);
            let data: Vec<u8> = slice.iter().map(|&b| if b { 1u8 } else { 0u8 }).collect();
            Ok((data, CasacoreDataType::TpBool))
        }
        ArrayValue::UInt8(a) => {
            let data = array_memory_order_values(a).as_ref().to_vec();
            Ok((data, CasacoreDataType::TpUChar))
        }
        ArrayValue::Int16(a) => {
            let slice = array_memory_order_values(a);
            let mut data = vec![0u8; slice.len() * 2];
            for (i, &v) in slice.iter().enumerate() {
                if big_endian {
                    write_i16_be(&mut data[i * 2..], v);
                } else {
                    write_i16_le(&mut data[i * 2..], v);
                }
            }
            Ok((data, CasacoreDataType::TpShort))
        }
        ArrayValue::UInt16(a) => {
            let slice = array_memory_order_values(a);
            let mut data = vec![0u8; slice.len() * 2];
            for (i, &v) in slice.iter().enumerate() {
                if big_endian {
                    write_u16_be(&mut data[i * 2..], v);
                } else {
                    write_u16_le(&mut data[i * 2..], v);
                }
            }
            Ok((data, CasacoreDataType::TpUShort))
        }
        ArrayValue::Int32(a) => {
            let slice = array_memory_order_values(a);
            let mut data = vec![0u8; slice.len() * 4];
            for (i, &v) in slice.iter().enumerate() {
                if big_endian {
                    write_i32_be(&mut data[i * 4..], v);
                } else {
                    write_i32_le(&mut data[i * 4..], v);
                }
            }
            Ok((data, CasacoreDataType::TpInt))
        }
        ArrayValue::UInt32(a) => {
            let slice = array_memory_order_values(a);
            let mut data = vec![0u8; slice.len() * 4];
            for (i, &v) in slice.iter().enumerate() {
                if big_endian {
                    write_u32_be(&mut data[i * 4..], v);
                } else {
                    write_u32_le(&mut data[i * 4..], v);
                }
            }
            Ok((data, CasacoreDataType::TpUInt))
        }
        ArrayValue::Float32(a) => {
            let slice = array_memory_order_values(a);
            let mut data = vec![0u8; slice.len() * 4];
            for (i, &v) in slice.iter().enumerate() {
                if big_endian {
                    write_f32_be(&mut data[i * 4..], v);
                } else {
                    write_f32_le(&mut data[i * 4..], v);
                }
            }
            Ok((data, CasacoreDataType::TpFloat))
        }
        ArrayValue::Float64(a) => {
            let slice = array_memory_order_values(a);
            let mut data = vec![0u8; slice.len() * 8];
            for (i, &v) in slice.iter().enumerate() {
                if big_endian {
                    write_f64_be(&mut data[i * 8..], v);
                } else {
                    write_f64_le(&mut data[i * 8..], v);
                }
            }
            Ok((data, CasacoreDataType::TpDouble))
        }
        ArrayValue::Int64(a) => {
            let slice = array_memory_order_values(a);
            let mut data = vec![0u8; slice.len() * 8];
            for (i, &v) in slice.iter().enumerate() {
                if big_endian {
                    write_i64_be(&mut data[i * 8..], v);
                } else {
                    write_i64_le(&mut data[i * 8..], v);
                }
            }
            Ok((data, CasacoreDataType::TpInt64))
        }
        ArrayValue::Complex32(a) => {
            let slice = array_memory_order_values(a);
            let mut data = vec![0u8; slice.len() * 8];
            for (i, &v) in slice.iter().enumerate() {
                if big_endian {
                    write_f32_be(&mut data[i * 8..], v.re);
                    write_f32_be(&mut data[i * 8 + 4..], v.im);
                } else {
                    write_f32_le(&mut data[i * 8..], v.re);
                    write_f32_le(&mut data[i * 8 + 4..], v.im);
                }
            }
            Ok((data, CasacoreDataType::TpComplex))
        }
        ArrayValue::Complex64(a) => {
            let slice = array_memory_order_values(a);
            let mut data = vec![0u8; slice.len() * 16];
            for (i, &v) in slice.iter().enumerate() {
                if big_endian {
                    write_f64_be(&mut data[i * 16..], v.re);
                    write_f64_be(&mut data[i * 16 + 8..], v.im);
                } else {
                    write_f64_le(&mut data[i * 16..], v.re);
                    write_f64_le(&mut data[i * 16 + 8..], v.im);
                }
            }
            Ok((data, CasacoreDataType::TpDComplex))
        }
        ArrayValue::String(_) => Err(StorageError::FormatMismatch(
            "string arrays not supported in tiled storage".to_string(),
        )),
    }
}

pub(super) fn array_memory_order_values<T: Clone>(array: &ArrayD<T>) -> Cow<'_, [T]> {
    if is_fortran_contiguous(array.shape(), array.strides())
        && let Some(slice) = array.as_slice_memory_order()
    {
        Cow::Borrowed(slice)
    } else {
        let fortran_ordered = array.t().as_standard_layout().into_owned();
        Cow::Owned(
            fortran_ordered
                .as_slice()
                .expect("contiguous after as_standard_layout")
                .to_vec(),
        )
    }
}
