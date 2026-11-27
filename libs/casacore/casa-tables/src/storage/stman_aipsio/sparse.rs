// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(crate) fn save_stman_file_rows_in_place(
    path: &Path,
    columns: &[ColumnDescContents],
    changed_values: &HashMap<&str, Vec<(usize, Option<Value>)>>,
    byte_order: ByteOrder,
) -> Result<bool, StorageError> {
    if changed_values.is_empty() {
        return Ok(true);
    }

    let layouts = parse_sparse_column_layouts(path, columns, byte_order)?;
    if layouts.len() != columns.len() {
        return Ok(false);
    }

    let mut main_file = OpenOptions::new().read(true).write(true).open(path)?;
    let array_file_path = indirect_array_file_path(path);
    let array_file_len = if array_file_path.exists() {
        Some(std::fs::metadata(&array_file_path)?.len())
    } else {
        None
    };
    let mut array_writer: Option<StManArrayFileWriter> = None;

    for (col_index, col_desc) in columns.iter().enumerate() {
        let Some(row_values) = changed_values.get(col_desc.col_name.as_str()) else {
            continue;
        };
        let Some(layout) = layouts[col_index].as_ref() else {
            return Ok(false);
        };
        match layout {
            SparseColumnLayout::ScalarBoolPacked { extents } => {
                for &(row_index, ref value) in row_values {
                    let Some(Value::Scalar(ScalarValue::Bool(flag))) = value else {
                        return Ok(false);
                    };
                    let (byte_pos, bit_index) = locate_bit_extent_slot(extents, row_index)?;
                    main_file.seek(SeekFrom::Start(byte_pos))?;
                    let mut current = [0u8; 1];
                    main_file.read_exact(&mut current)?;
                    if *flag {
                        current[0] |= 1 << bit_index;
                    } else {
                        current[0] &= !(1 << bit_index);
                    }
                    main_file.seek(SeekFrom::Start(byte_pos))?;
                    main_file.write_all(&current)?;
                }
            }
            SparseColumnLayout::IndirectArrayOffsets { data_type, extents } => {
                let existing_len = array_file_len.unwrap_or(0);
                if existing_len >= LARGE_OFFSET_MARKER as u64 {
                    return Ok(false);
                }
                let writer = match array_writer.as_mut() {
                    Some(writer) => writer,
                    None => {
                        array_writer = Some(StManArrayFileWriter::open_append(
                            &array_file_path,
                            byte_order == ByteOrder::BigEndian,
                        )?);
                        array_writer.as_mut().expect("writer just created")
                    }
                };
                for &(row_index, ref value) in row_values {
                    let offset = match value {
                        Some(value @ Value::Array(_)) => writer.write_array(value, *data_type)?,
                        None => 0,
                        _ => return Ok(false),
                    };
                    let offset_u32 = u32::try_from(offset).map_err(|_| {
                        StorageError::FormatMismatch(format!(
                            "sparse StManAipsIO offset exceeds u32 for column {} row {}",
                            col_desc.col_name, row_index
                        ))
                    })?;
                    let slot = locate_extent_slot(extents, row_index)?;
                    main_file.seek(SeekFrom::Start(slot))?;
                    main_file.write_all(&offset_u32.to_be_bytes())?;
                }
            }
            SparseColumnLayout::ScalarFixed { data_type, extents } => {
                for &(row_index, ref value) in row_values {
                    let Some(Value::Scalar(scalar)) = value else {
                        return Ok(false);
                    };
                    let encoded = encode_fixed_scalar_value(*data_type, scalar)?;
                    let slot = locate_extent_slot(extents, row_index)?;
                    main_file.seek(SeekFrom::Start(slot))?;
                    main_file.write_all(&encoded)?;
                }
            }
            SparseColumnLayout::DirectArrayFixed { data_type, extents } => {
                for &(row_index, ref value) in row_values {
                    let Some(Value::Array(array)) = value else {
                        return Ok(false);
                    };
                    let encoded = encode_fixed_array_row(*data_type, array)?;
                    let slot = locate_extent_slot(extents, row_index)?;
                    main_file.seek(SeekFrom::Start(slot))?;
                    main_file.write_all(&encoded)?;
                }
            }
        }
    }

    if let Some(writer) = array_writer.as_mut() {
        writer.finish()?;
    }
    main_file.flush()?;
    Ok(true)
}

pub(crate) fn read_stman_array_column_rows(
    path: &Path,
    columns: &[ColumnDescContents],
    target_col_idx: usize,
    selected_rows: &[usize],
    byte_order: ByteOrder,
) -> Result<Option<Vec<Option<ArrayValue>>>, StorageError> {
    if selected_rows.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let layouts = parse_sparse_column_layouts(path, columns, byte_order)?;
    let Some(layout) = layouts
        .get(target_col_idx)
        .and_then(|layout| layout.as_ref())
    else {
        return Ok(None);
    };
    let column = columns.get(target_col_idx).ok_or_else(|| {
        StorageError::FormatMismatch(format!(
            "target StManAipsIO array column index {target_col_idx} is out of range"
        ))
    })?;

    let mut requests: Vec<(usize, usize)> = selected_rows
        .iter()
        .copied()
        .enumerate()
        .map(|(out_idx, row_index)| (row_index, out_idx))
        .collect();
    requests.sort_unstable_by_key(|&(row_index, _)| row_index);
    let mut main_file = std::fs::File::open(path)?;

    match layout {
        SparseColumnLayout::IndirectArrayOffsets { data_type, extents } => {
            let array_file_path = indirect_array_file_path(path);
            if std::fs::metadata(&array_file_path)
                .map(|meta| meta.len())
                .unwrap_or(0)
                >= LARGE_OFFSET_MARKER as u64
            {
                return Ok(None);
            }

            let mut offsets = vec![0u32; selected_rows.len()];
            for (row_index, out_idx) in requests {
                let slot = locate_extent_slot(extents, row_index)?;
                main_file.seek(SeekFrom::Start(slot))?;
                offsets[out_idx] = read_be_u32(&mut main_file)?;
            }

            let mut reader =
                StManArrayFileReader::open(&array_file_path, byte_order == ByteOrder::BigEndian)?;
            let mut values = Vec::with_capacity(offsets.len());
            for offset in offsets {
                let value = reader
                    .read_array_at(offset as i64, *data_type)?
                    .map(|value| match value {
                        Value::Array(array) => Ok(array),
                        other => Err(StorageError::FormatMismatch(format!(
                            "expected array value in indirect StManAipsIO column, found {:?}",
                            other.kind()
                        ))),
                    })
                    .transpose()?;
                values.push(value);
            }
            Ok(Some(values))
        }
        SparseColumnLayout::DirectArrayFixed { data_type, extents } => {
            let shape: Vec<usize> = column.shape.iter().map(|&dim| dim as usize).collect();
            if shape.is_empty() {
                return Ok(None);
            }
            let row_width = extents
                .first()
                .map(|extent| extent.row_width_bytes)
                .ok_or_else(|| {
                    StorageError::FormatMismatch(format!(
                        "direct StManAipsIO array column '{}' has no extents",
                        column.col_name
                    ))
                })?;
            let mut row_bytes = vec![0u8; row_width];
            let mut values = vec![None; selected_rows.len()];
            for (row_index, out_idx) in requests {
                let slot = locate_extent_slot(extents, row_index)?;
                main_file.seek(SeekFrom::Start(slot))?;
                main_file.read_exact(&mut row_bytes)?;
                values[out_idx] = Some(decode_fixed_array_value(
                    *data_type,
                    &shape,
                    &row_bytes,
                    &column.col_name,
                )?);
            }
            Ok(Some(values))
        }
        _ => Ok(None),
    }
}

pub(crate) fn read_stman_scalar_column_rows(
    path: &Path,
    columns: &[ColumnDescContents],
    target_col_idx: usize,
    selected_rows: &[usize],
    byte_order: ByteOrder,
) -> Result<Option<Vec<Option<ScalarValue>>>, StorageError> {
    if selected_rows.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let layouts = parse_sparse_column_layouts(path, columns, byte_order)?;
    let Some(layout) = layouts
        .get(target_col_idx)
        .and_then(|layout| layout.as_ref())
    else {
        return Ok(None);
    };
    let column = columns.get(target_col_idx).ok_or_else(|| {
        StorageError::FormatMismatch(format!(
            "target StManAipsIO scalar column index {target_col_idx} is out of range"
        ))
    })?;

    let mut main_file = std::fs::File::open(path)?;
    let mut requests: Vec<(usize, usize)> = selected_rows
        .iter()
        .copied()
        .enumerate()
        .map(|(out_idx, row_index)| (row_index, out_idx))
        .collect();
    requests.sort_unstable_by_key(|&(row_index, _)| row_index);

    let mut values = vec![None; selected_rows.len()];
    match layout {
        SparseColumnLayout::ScalarBoolPacked { extents } => {
            for (row_index, out_idx) in requests {
                let (slot, bit_offset) = locate_bit_extent_slot(extents, row_index)?;
                main_file.seek(SeekFrom::Start(slot))?;
                let mut byte = [0u8; 1];
                main_file.read_exact(&mut byte)?;
                let scalar = ScalarValue::Bool(((byte[0] >> bit_offset) & 1) != 0);
                values[out_idx] = if (column.option & 2) != 0
                    && scalar_value_is_default(
                        &Value::Scalar(scalar.clone()),
                        column.require_primitive_type()?,
                    ) {
                    None
                } else {
                    Some(scalar)
                };
            }
        }
        SparseColumnLayout::ScalarFixed { data_type, extents } => {
            let row_width = scalar_fixed_width_bytes(*data_type).ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "unsupported direct StManAipsIO scalar type {data_type:?}"
                ))
            })?;
            let mut raw = vec![0u8; row_width];
            for (row_index, out_idx) in requests {
                let slot = locate_extent_slot(extents, row_index)?;
                main_file.seek(SeekFrom::Start(slot))?;
                main_file.read_exact(&mut raw)?;
                let scalar = decode_fixed_scalar_value(*data_type, &raw)?;
                values[out_idx] = if (column.option & 2) != 0
                    && scalar_value_is_default(
                        &Value::Scalar(scalar.clone()),
                        column.require_primitive_type()?,
                    ) {
                    None
                } else {
                    Some(scalar)
                };
            }
        }
        _ => return Ok(None),
    }

    Ok(Some(values))
}

pub(crate) fn read_stman_scalar_column(
    path: &Path,
    columns: &[ColumnDescContents],
    target_col_idx: usize,
    byte_order: ByteOrder,
) -> Result<Option<Vec<Option<ScalarValue>>>, StorageError> {
    let layouts = parse_sparse_column_layouts(path, columns, byte_order)?;
    let Some(layout) = layouts
        .get(target_col_idx)
        .and_then(|layout| layout.as_ref())
    else {
        return Ok(None);
    };
    let column = columns.get(target_col_idx).ok_or_else(|| {
        StorageError::FormatMismatch(format!(
            "target StManAipsIO scalar column index {target_col_idx} is out of range"
        ))
    })?;

    let mut file = std::fs::File::open(path)?;
    let mut values = Vec::new();
    match layout {
        SparseColumnLayout::ScalarBoolPacked { extents } => {
            for extent in extents {
                file.seek(SeekFrom::Start(extent.values_start))?;
                let mut packed = vec![0u8; extent.row_count.div_ceil(8)];
                file.read_exact(&mut packed)?;
                for row_offset in 0..extent.row_count {
                    let byte = packed[row_offset / 8];
                    let flag = ((byte >> (row_offset % 8)) & 1) != 0;
                    let scalar = ScalarValue::Bool(flag);
                    if (column.option & 2) != 0
                        && scalar_value_is_default(
                            &Value::Scalar(scalar.clone()),
                            column.require_primitive_type()?,
                        )
                    {
                        values.push(None);
                    } else {
                        values.push(Some(scalar));
                    }
                }
            }
        }
        SparseColumnLayout::ScalarFixed { data_type, extents } => {
            for extent in extents {
                file.seek(SeekFrom::Start(extent.values_start))?;
                let mut raw = vec![0u8; extent.row_count * extent.row_width_bytes];
                file.read_exact(&mut raw)?;
                for chunk in raw.chunks_exact(extent.row_width_bytes) {
                    let scalar = decode_fixed_scalar_value(*data_type, chunk)?;
                    if (column.option & 2) != 0
                        && scalar_value_is_default(
                            &Value::Scalar(scalar.clone()),
                            column.require_primitive_type()?,
                        )
                    {
                        values.push(None);
                    } else {
                        values.push(Some(scalar));
                    }
                }
            }
        }
        _ => return Ok(None),
    }
    Ok(Some(values))
}

pub(super) fn parse_sparse_column_layouts(
    path: &Path,
    columns: &[ColumnDescContents],
    byte_order: ByteOrder,
) -> Result<Vec<Option<SparseColumnLayout>>, StorageError> {
    if byte_order != ByteOrder::BigEndian {
        return Ok(vec![None; columns.len()]);
    }

    let mut file = std::fs::File::open(path)?;
    let magic = read_be_u32(&mut file)?;
    if magic != AIPSIO_TOP_LEVEL_MAGIC {
        return Err(StorageError::FormatMismatch(format!(
            "StManAipsIO file missing top-level magic: {magic:#x}"
        )));
    }
    let top = read_object_header(&mut file)?;
    if top.type_name != "StManAipsIO" {
        return Err(StorageError::FormatMismatch(format!(
            "expected StManAipsIO top-level object, found {}",
            top.type_name
        )));
    }

    let _name = read_be_string(&mut file)?;
    let _seq_nr = read_be_u32(&mut file)?;
    let _uniq_nr = read_be_u32(&mut file)?;
    let nrrow = read_be_u32(&mut file)? as usize;
    let ncol = read_be_u32(&mut file)? as usize;
    if ncol != columns.len() {
        return Err(StorageError::FormatMismatch(format!(
            "StManAipsIO column count mismatch: file has {ncol}, expected {}",
            columns.len()
        )));
    }

    let mut data_types = Vec::with_capacity(ncol);
    for _ in 0..ncol {
        let dt_i32 = read_be_i32(&mut file)?;
        let dt = CasacoreDataType::from_i32(dt_i32).ok_or_else(|| {
            StorageError::FormatMismatch(format!("unknown StManAipsIO dtype tag: {dt_i32}"))
        })?;
        data_types.push(dt);
    }

    let mut layouts = Vec::with_capacity(ncol);
    for (col_desc, data_type) in columns.iter().zip(data_types) {
        let column_header = read_object_header(&mut file)?;
        let layout = match column_header.type_name.as_str() {
            "StManColumnIndArrayAipsIO" => {
                let _compat_dtype = read_be_i32(&mut file)?;
                let _seqnr = read_be_i32(&mut file)?;
                let inner = read_object_header(&mut file)?;
                if inner.type_name != "StManColumnAipsIO" {
                    None
                } else {
                    let stored_nrrow = read_be_u32(&mut file)? as usize;
                    if stored_nrrow != nrrow {
                        None
                    } else {
                        parse_indirect_offsets_layout(
                            &mut file,
                            stored_nrrow,
                            data_type,
                            std::fs::metadata(indirect_array_file_path(path))
                                .is_ok_and(|metadata| metadata.len() >= LARGE_OFFSET_MARKER as u64),
                        )?
                    }
                }
            }
            "StManColumnAipsIO" => {
                let stored_nrrow = read_be_u32(&mut file)? as usize;
                if stored_nrrow != nrrow {
                    None
                } else {
                    parse_scalar_layout(&mut file, stored_nrrow, data_type)?
                }
            }
            "StManColumnArrayAipsIO" => {
                let inner = read_object_header(&mut file)?;
                if inner.type_name != "StManColumnAipsIO" {
                    None
                } else {
                    let stored_nrrow = read_be_u32(&mut file)? as usize;
                    if stored_nrrow != nrrow {
                        None
                    } else {
                        parse_direct_array_layout(&mut file, stored_nrrow, col_desc, data_type)?
                    }
                }
            }
            _ => None,
        };
        file.seek(SeekFrom::Start(column_header.object_end))?;
        layouts.push(layout);
    }

    file.seek(SeekFrom::Start(top.object_end))?;
    Ok(layouts)
}

pub(super) fn parse_scalar_layout(
    file: &mut std::fs::File,
    nrrow: usize,
    data_type: CasacoreDataType,
) -> Result<Option<SparseColumnLayout>, StorageError> {
    if data_type == CasacoreDataType::TpBool {
        let mut extents = Vec::new();
        let mut row_start = 0usize;
        while row_start < nrrow {
            let raw_count = read_be_u32(file)? as usize;
            let row_count = if raw_count == 0 {
                nrrow - row_start
            } else {
                raw_count
            };
            let stored_count = read_be_u32(file)? as usize;
            if stored_count != row_count {
                return Ok(None);
            }
            let values_start = file.stream_position()?;
            let packed_bytes = row_count.div_ceil(8);
            file.seek(SeekFrom::Current(packed_bytes as i64))?;
            extents.push(BitExtentLayout {
                row_start,
                row_count,
                values_start,
            });
            row_start += row_count;
        }
        return Ok(Some(SparseColumnLayout::ScalarBoolPacked { extents }));
    }

    let Some(row_width_bytes) = scalar_fixed_width_bytes(data_type) else {
        return Ok(None);
    };
    let mut extents = Vec::new();
    let mut row_start = 0usize;
    while row_start < nrrow {
        let raw_count = read_be_u32(file)? as usize;
        let row_count = if raw_count == 0 {
            nrrow - row_start
        } else {
            raw_count
        };
        let stored_count = read_be_u32(file)? as usize;
        if stored_count != row_count {
            return Ok(None);
        }
        let values_start = file.stream_position()?;
        file.seek(SeekFrom::Current((row_count * row_width_bytes) as i64))?;
        extents.push(ExtentLayout {
            row_start,
            row_count,
            values_start,
            row_width_bytes,
        });
        row_start += row_count;
    }
    Ok(Some(SparseColumnLayout::ScalarFixed { data_type, extents }))
}

pub(super) fn parse_direct_array_layout(
    file: &mut std::fs::File,
    nrrow: usize,
    col_desc: &ColumnDescContents,
    data_type: CasacoreDataType,
) -> Result<Option<SparseColumnLayout>, StorageError> {
    let Some(elem_width) = scalar_fixed_width_bytes(data_type) else {
        return Ok(None);
    };
    let elements_per_row: usize = col_desc.shape.iter().map(|&dim| dim as usize).product();
    if elements_per_row == 0 {
        return Ok(None);
    }
    let row_width_bytes = elements_per_row.checked_mul(elem_width).ok_or_else(|| {
        StorageError::FormatMismatch("direct-array row width overflow".to_string())
    })?;
    let mut extents = Vec::new();
    let mut row_start = 0usize;
    while row_start < nrrow {
        let raw_count = read_be_u32(file)? as usize;
        let row_count = if raw_count == 0 {
            nrrow - row_start
        } else {
            raw_count
        };
        let total_elements = read_be_u32(file)? as usize;
        if total_elements != row_count * elements_per_row {
            return Ok(None);
        }
        let values_start = file.stream_position()?;
        file.seek(SeekFrom::Current((row_count * row_width_bytes) as i64))?;
        extents.push(ExtentLayout {
            row_start,
            row_count,
            values_start,
            row_width_bytes,
        });
        row_start += row_count;
    }
    Ok(Some(SparseColumnLayout::DirectArrayFixed {
        data_type,
        extents,
    }))
}

pub(super) fn read_object_header(file: &mut std::fs::File) -> Result<ObjectHeader, StorageError> {
    let len_pos = file.stream_position()?;
    let len = read_be_u32(file)?;
    let type_name = read_be_string(file)?;
    let version = read_be_u32(file)?;
    let payload_start = file.stream_position()?;
    let object_end = len_pos + len as u64;
    Ok(ObjectHeader {
        len,
        type_name,
        version,
        payload_start,
        object_end,
    })
}

pub(super) fn locate_extent_slot(
    extents: &[ExtentLayout],
    row_index: usize,
) -> Result<u64, StorageError> {
    for extent in extents {
        if row_index >= extent.row_start && row_index < extent.row_start + extent.row_count {
            return Ok(extent.values_start
                + ((row_index - extent.row_start) * extent.row_width_bytes) as u64);
        }
    }
    Err(StorageError::FormatMismatch(format!(
        "row {row_index} is outside sparse-patch extents"
    )))
}

pub(super) fn locate_bit_extent_slot(
    extents: &[BitExtentLayout],
    row_index: usize,
) -> Result<(u64, usize), StorageError> {
    for extent in extents {
        if row_index >= extent.row_start && row_index < extent.row_start + extent.row_count {
            let row_offset = row_index - extent.row_start;
            return Ok((
                extent.values_start + (row_offset / 8) as u64,
                row_offset % 8,
            ));
        }
    }
    Err(StorageError::FormatMismatch(format!(
        "row {row_index} is outside sparse-patch bit extents"
    )))
}

pub(super) fn scalar_fixed_width_bytes(data_type: CasacoreDataType) -> Option<usize> {
    match data_type {
        CasacoreDataType::TpBool | CasacoreDataType::TpUChar | CasacoreDataType::TpChar => Some(1),
        CasacoreDataType::TpShort | CasacoreDataType::TpUShort => Some(2),
        CasacoreDataType::TpInt | CasacoreDataType::TpUInt | CasacoreDataType::TpFloat => Some(4),
        CasacoreDataType::TpDouble | CasacoreDataType::TpInt64 | CasacoreDataType::TpComplex => {
            Some(8)
        }
        CasacoreDataType::TpDComplex => Some(16),
        _ => None,
    }
}

pub(super) fn decode_fixed_scalar_value(
    data_type: CasacoreDataType,
    bytes: &[u8],
) -> Result<ScalarValue, StorageError> {
    let scalar = match data_type {
        CasacoreDataType::TpUChar | CasacoreDataType::TpChar => {
            ScalarValue::UInt8(*bytes.first().ok_or_else(|| {
                StorageError::FormatMismatch("missing u8 scalar bytes".to_string())
            })?)
        }
        CasacoreDataType::TpShort => {
            ScalarValue::Int16(i16::from_be_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid i16 scalar width".to_string())
            })?))
        }
        CasacoreDataType::TpUShort => {
            ScalarValue::UInt16(u16::from_be_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid u16 scalar width".to_string())
            })?))
        }
        CasacoreDataType::TpInt => {
            ScalarValue::Int32(i32::from_be_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid i32 scalar width".to_string())
            })?))
        }
        CasacoreDataType::TpUInt => {
            ScalarValue::UInt32(u32::from_be_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid u32 scalar width".to_string())
            })?))
        }
        CasacoreDataType::TpInt64 => {
            ScalarValue::Int64(i64::from_be_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid i64 scalar width".to_string())
            })?))
        }
        CasacoreDataType::TpFloat => {
            ScalarValue::Float32(f32::from_be_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid f32 scalar width".to_string())
            })?))
        }
        CasacoreDataType::TpDouble => {
            ScalarValue::Float64(f64::from_be_bytes(bytes.try_into().map_err(|_| {
                StorageError::FormatMismatch("invalid f64 scalar width".to_string())
            })?))
        }
        CasacoreDataType::TpComplex => {
            if bytes.len() != 8 {
                return Err(StorageError::FormatMismatch(
                    "invalid complex32 scalar width".to_string(),
                ));
            }
            ScalarValue::Complex32(casa_core::Complex32 {
                re: f32::from_be_bytes(bytes[0..4].try_into().expect("slice width checked")),
                im: f32::from_be_bytes(bytes[4..8].try_into().expect("slice width checked")),
            })
        }
        CasacoreDataType::TpDComplex => {
            if bytes.len() != 16 {
                return Err(StorageError::FormatMismatch(
                    "invalid complex64 scalar width".to_string(),
                ));
            }
            ScalarValue::Complex64(casa_core::Complex64 {
                re: f64::from_be_bytes(bytes[0..8].try_into().expect("slice width checked")),
                im: f64::from_be_bytes(bytes[8..16].try_into().expect("slice width checked")),
            })
        }
        other => {
            return Err(StorageError::FormatMismatch(format!(
                "unsupported direct StManAipsIO scalar decode for {other:?}"
            )));
        }
    };
    Ok(scalar)
}

pub(super) fn decode_fixed_array_value(
    data_type: CasacoreDataType,
    shape: &[usize],
    bytes: &[u8],
    column_name: &str,
) -> Result<ArrayValue, StorageError> {
    let elem_width = scalar_fixed_width_bytes(data_type).ok_or_else(|| {
        StorageError::FormatMismatch(format!(
            "unsupported direct StManAipsIO array type {data_type:?}"
        ))
    })?;
    let elements_per_row: usize = shape.iter().product();
    if bytes.len() != elements_per_row * elem_width {
        return Err(StorageError::FormatMismatch(format!(
            "direct StManAipsIO array column '{column_name}' row width {} does not match shape {:?} and type {:?}",
            bytes.len(),
            shape,
            data_type
        )));
    }

    match data_type {
        CasacoreDataType::TpBool => {
            let values = bytes.iter().map(|byte| *byte != 0).collect();
            Ok(ArrayValue::Bool(array_from_fixed_row(shape, values)?))
        }
        CasacoreDataType::TpUChar | CasacoreDataType::TpChar => Ok(ArrayValue::UInt8(
            array_from_fixed_row(shape, bytes.to_vec())?,
        )),
        CasacoreDataType::TpShort => {
            let mut values = Vec::with_capacity(elements_per_row);
            for chunk in bytes.chunks_exact(elem_width) {
                values.push(i16::from_be_bytes(chunk.try_into().expect("chunk width")));
            }
            Ok(ArrayValue::Int16(array_from_fixed_row(shape, values)?))
        }
        CasacoreDataType::TpUShort => {
            let mut values = Vec::with_capacity(elements_per_row);
            for chunk in bytes.chunks_exact(elem_width) {
                values.push(u16::from_be_bytes(chunk.try_into().expect("chunk width")));
            }
            Ok(ArrayValue::UInt16(array_from_fixed_row(shape, values)?))
        }
        CasacoreDataType::TpInt => {
            let mut values = Vec::with_capacity(elements_per_row);
            for chunk in bytes.chunks_exact(elem_width) {
                values.push(i32::from_be_bytes(chunk.try_into().expect("chunk width")));
            }
            Ok(ArrayValue::Int32(array_from_fixed_row(shape, values)?))
        }
        CasacoreDataType::TpUInt => {
            let mut values = Vec::with_capacity(elements_per_row);
            for chunk in bytes.chunks_exact(elem_width) {
                values.push(u32::from_be_bytes(chunk.try_into().expect("chunk width")));
            }
            Ok(ArrayValue::UInt32(array_from_fixed_row(shape, values)?))
        }
        CasacoreDataType::TpInt64 => {
            let mut values = Vec::with_capacity(elements_per_row);
            for chunk in bytes.chunks_exact(elem_width) {
                values.push(i64::from_be_bytes(chunk.try_into().expect("chunk width")));
            }
            Ok(ArrayValue::Int64(array_from_fixed_row(shape, values)?))
        }
        CasacoreDataType::TpFloat => {
            let mut values = Vec::with_capacity(elements_per_row);
            for chunk in bytes.chunks_exact(elem_width) {
                values.push(f32::from_be_bytes(chunk.try_into().expect("chunk width")));
            }
            Ok(ArrayValue::Float32(array_from_fixed_row(shape, values)?))
        }
        CasacoreDataType::TpDouble => {
            let mut values = Vec::with_capacity(elements_per_row);
            for chunk in bytes.chunks_exact(elem_width) {
                values.push(f64::from_be_bytes(chunk.try_into().expect("chunk width")));
            }
            Ok(ArrayValue::Float64(array_from_fixed_row(shape, values)?))
        }
        CasacoreDataType::TpComplex => {
            let mut values = Vec::with_capacity(elements_per_row);
            for chunk in bytes.chunks_exact(elem_width) {
                values.push(casa_core::Complex32 {
                    re: f32::from_be_bytes(chunk[0..4].try_into().expect("chunk width")),
                    im: f32::from_be_bytes(chunk[4..8].try_into().expect("chunk width")),
                });
            }
            Ok(ArrayValue::Complex32(array_from_fixed_row(shape, values)?))
        }
        CasacoreDataType::TpDComplex => {
            let mut values = Vec::with_capacity(elements_per_row);
            for chunk in bytes.chunks_exact(elem_width) {
                values.push(casa_core::Complex64 {
                    re: f64::from_be_bytes(chunk[0..8].try_into().expect("chunk width")),
                    im: f64::from_be_bytes(chunk[8..16].try_into().expect("chunk width")),
                });
            }
            Ok(ArrayValue::Complex64(array_from_fixed_row(shape, values)?))
        }
        other => Err(StorageError::FormatMismatch(format!(
            "unsupported direct StManAipsIO array decode for {other:?}"
        ))),
    }
}

pub(super) fn array_from_fixed_row<T: Clone>(
    shape: &[usize],
    values: Vec<T>,
) -> Result<ArrayD<T>, StorageError> {
    ArrayD::from_shape_vec(IxDyn(shape).f(), values)
        .map_err(|err| StorageError::FormatMismatch(format!("array shape: {err}")))
}

pub(super) fn encode_fixed_scalar_value(
    data_type: CasacoreDataType,
    value: &ScalarValue,
) -> Result<Vec<u8>, StorageError> {
    let bytes = match (data_type, value) {
        (CasacoreDataType::TpBool, ScalarValue::Bool(v)) => vec![u8::from(*v)],
        (CasacoreDataType::TpUChar, ScalarValue::UInt8(v))
        | (CasacoreDataType::TpChar, ScalarValue::UInt8(v)) => vec![*v],
        (CasacoreDataType::TpShort, ScalarValue::Int16(v)) => v.to_be_bytes().to_vec(),
        (CasacoreDataType::TpUShort, ScalarValue::UInt16(v)) => v.to_be_bytes().to_vec(),
        (CasacoreDataType::TpInt, ScalarValue::Int32(v)) => v.to_be_bytes().to_vec(),
        (CasacoreDataType::TpUInt, ScalarValue::UInt32(v)) => v.to_be_bytes().to_vec(),
        (CasacoreDataType::TpInt64, ScalarValue::Int64(v)) => v.to_be_bytes().to_vec(),
        (CasacoreDataType::TpFloat, ScalarValue::Float32(v)) => v.to_be_bytes().to_vec(),
        (CasacoreDataType::TpDouble, ScalarValue::Float64(v)) => v.to_be_bytes().to_vec(),
        (CasacoreDataType::TpComplex, ScalarValue::Complex32(v)) => {
            let mut bytes = Vec::with_capacity(8);
            bytes.extend_from_slice(&v.re.to_be_bytes());
            bytes.extend_from_slice(&v.im.to_be_bytes());
            bytes
        }
        (CasacoreDataType::TpDComplex, ScalarValue::Complex64(v)) => {
            let mut bytes = Vec::with_capacity(16);
            bytes.extend_from_slice(&v.re.to_be_bytes());
            bytes.extend_from_slice(&v.im.to_be_bytes());
            bytes
        }
        _ => {
            return Err(StorageError::FormatMismatch(format!(
                "unsupported sparse scalar patch for type {data_type:?} value {value:?}"
            )));
        }
    };
    Ok(bytes)
}

pub(super) fn encode_fixed_array_row(
    data_type: CasacoreDataType,
    value: &ArrayValue,
) -> Result<Vec<u8>, StorageError> {
    let mut encoded = Vec::new();
    match (data_type, value) {
        (CasacoreDataType::TpBool, _) | (_, ArrayValue::String(_)) => {
            return Err(StorageError::FormatMismatch(format!(
                "unsupported sparse direct-array patch for {data_type:?}"
            )));
        }
        (CasacoreDataType::TpUChar, ArrayValue::UInt8(arr))
        | (CasacoreDataType::TpChar, ArrayValue::UInt8(arr)) => {
            encoded.extend(fortran_flat_iter(arr))
        }
        (CasacoreDataType::TpShort, ArrayValue::Int16(arr)) => {
            for value in fortran_flat_iter(arr) {
                encoded.extend_from_slice(&value.to_be_bytes());
            }
        }
        (CasacoreDataType::TpUShort, ArrayValue::UInt16(arr)) => {
            for value in fortran_flat_iter(arr) {
                encoded.extend_from_slice(&value.to_be_bytes());
            }
        }
        (CasacoreDataType::TpInt, ArrayValue::Int32(arr)) => {
            for value in fortran_flat_iter(arr) {
                encoded.extend_from_slice(&value.to_be_bytes());
            }
        }
        (CasacoreDataType::TpUInt, ArrayValue::UInt32(arr)) => {
            for value in fortran_flat_iter(arr) {
                encoded.extend_from_slice(&value.to_be_bytes());
            }
        }
        (CasacoreDataType::TpInt64, ArrayValue::Int64(arr)) => {
            for value in fortran_flat_iter(arr) {
                encoded.extend_from_slice(&value.to_be_bytes());
            }
        }
        (CasacoreDataType::TpFloat, ArrayValue::Float32(arr)) => {
            for value in fortran_flat_iter(arr) {
                encoded.extend_from_slice(&value.to_be_bytes());
            }
        }
        (CasacoreDataType::TpDouble, ArrayValue::Float64(arr)) => {
            for value in fortran_flat_iter(arr) {
                encoded.extend_from_slice(&value.to_be_bytes());
            }
        }
        (CasacoreDataType::TpComplex, ArrayValue::Complex32(arr)) => {
            for value in fortran_flat_iter(arr) {
                encoded.extend_from_slice(&value.re.to_be_bytes());
                encoded.extend_from_slice(&value.im.to_be_bytes());
            }
        }
        (CasacoreDataType::TpDComplex, ArrayValue::Complex64(arr)) => {
            for value in fortran_flat_iter(arr) {
                encoded.extend_from_slice(&value.re.to_be_bytes());
                encoded.extend_from_slice(&value.im.to_be_bytes());
            }
        }
        _ => {
            return Err(StorageError::FormatMismatch(format!(
                "unsupported sparse direct-array patch for type {data_type:?}"
            )));
        }
    }
    Ok(encoded)
}
