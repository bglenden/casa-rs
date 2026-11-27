// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(crate) fn write_ref_table_stream(
    mut io: AipsIo,
    contents: &RefTableDatContents,
) -> Result<(), StorageError> {
    let limit = u64::from(u32::MAX);
    let wide = contents.nrrow >= limit
        || contents.parent_nrrow >= limit
        || contents.row_map.iter().any(|&row| row >= limit);
    io.putstart("Table", if contents.nrrow >= limit { 3 } else { 2 })?;
    if contents.nrrow >= limit {
        io.put_u64(contents.nrrow)?;
    } else {
        io.put_u32(contents.nrrow as u32)?;
    }
    io.put_u32(if contents.big_endian { 0 } else { 1 })?;
    io.put_string("RefTable")?;

    io.putstart("RefTable", if wide { 3 } else { 2 })?;

    // Parent table relative path.
    io.put_string(&contents.parent_relative_path)?;

    // Column name map: C++ serializes std::map<String, String> as a
    // "SimpleOrderedMap" AipsIO object.
    io.putstart("SimpleOrderedMap", 1)?;
    io.put_string("")?; // old default value (unused)
    io.put_u32(contents.column_name_map.len() as u32)?;
    io.put_u32(1)?; // old increment (unused)
    for (view_name, parent_name) in &contents.column_name_map {
        io.put_string(view_name)?;
        io.put_string(parent_name)?;
    }
    io.putend()?;

    // Column names (version > 1): C++ writes as an "Array" AipsIO object (v3).
    // Format: ndim(u32), shape[ndim](u32...), count(u32), then strings.
    io.putstart("Array", 3)?;
    io.put_u32(1)?; // ndim = 1 (Vector)
    io.put_u32(contents.column_names.len() as u32)?; // shape[0]
    io.put_u32(contents.column_names.len() as u32)?; // element count
    for name in &contents.column_names {
        io.put_string(name)?;
    }
    io.putend()?;

    if wide {
        io.put_u64(contents.parent_nrrow)?;
    } else {
        io.put_u32(contents.parent_nrrow as u32)?;
    }

    // Row order flag.
    io.put_bool(contents.row_order)?;

    // This RefTable's row count.
    if wide {
        io.put_u64(contents.nrrow)?;
    } else {
        io.put_u32(contents.nrrow as u32)?;
    }

    // Row number vector (chunked at 2^20, no count prefix).
    let max_chunk: usize = 1 << 20;
    for chunk in contents.row_map.chunks(max_chunk) {
        if wide {
            io.put_u64_slice(chunk, false)?;
        } else {
            let chunk32: Vec<u32> = chunk.iter().map(|&row| row as u32).collect();
            io.put_u32_slice(&chunk32, false)?;
        }
    }

    io.putend()?; // RefTable

    io.putend()?; // Table
    io.close()?;
    Ok(())
}

/// Write table.dat for a ConcatTable.
///
/// Binary layout matches C++ `ConcatTable::writeConcatTable`:
/// ```text
/// putstart("Table", 2)
///   nrrow (u32), endian (u32), "ConcatTable" (String)
///   putstart("ConcatTable", 0)
///     ntable (u32), table_path[0..ntable] (String each)
///     putstart("Block", 1)                 // Block<String> subtable names
///       count (u32), sub_table_name[0..count] (String each)
///     putend()
///   putend()
/// putend()
/// ```
pub(crate) fn write_concat_table_dat(
    path: &Path,
    contents: &ConcatTableDatContents,
) -> Result<(), StorageError> {
    let mut io = AipsIo::open(path, AipsOpenOption::New)?;

    // BaseTable uses version 3 once the row count exceeds a CASA Int.
    let wide = contents.nrrow > i32::MAX as u64;
    io.putstart("Table", if wide { 3 } else { 2 })?;
    if wide {
        io.put_u64(contents.nrrow)?;
    } else {
        io.put_u32(contents.nrrow as u32)?;
    }
    io.put_u32(if contents.big_endian { 0 } else { 1 })?;
    io.put_string("ConcatTable")?;

    // ConcatTable payload — version 0.
    io.putstart("ConcatTable", 0)?;

    // Number of constituent tables and their relative paths.
    io.put_u32(contents.table_paths.len() as u32)?;
    for table_path in &contents.table_paths {
        io.put_string(table_path)?;
    }

    // Block<String> subtable names: C++ serializes as "Block" AipsIO object (v1).
    io.putstart("Block", 1)?;
    io.put_u32(contents.sub_table_names.len() as u32)?;
    for name in &contents.sub_table_names {
        io.put_string(name)?;
    }
    io.putend()?; // Block

    io.putend()?; // ConcatTable

    io.putend()?; // Table
    io.close()?;
    Ok(())
}

pub(super) fn write_table_desc(
    io: &mut AipsIo,
    desc: &TableDescContents,
) -> Result<(), StorageError> {
    io.putstart("TableDesc", 2)?;
    io.put_string(&desc.name)?;
    io.put_string(&desc.version)?;
    io.put_string(&desc.comment)?;

    write_table_record(io, &desc.table_keywords)?;
    write_table_record(io, &desc.private_keywords)?;

    // ColumnDescSet
    io.put_u32(desc.columns.len() as u32)?;
    for col in &desc.columns {
        write_column_desc(io, col)?;
    }

    io.putend()?;
    Ok(())
}

pub(super) fn write_column_desc(
    io: &mut AipsIo,
    col: &ColumnDescContents,
) -> Result<(), StorageError> {
    // ColumnDesc wrapper
    io.put_u32(1)?; // version=1
    io.put_string(&col.class_name)?;

    // BaseColumnDesc
    io.put_u32(1)?; // version=1
    io.put_string(&col.col_name)?;
    io.put_string(&col.comment)?;
    io.put_string(&col.data_manager_type)?;
    io.put_string(&col.data_manager_group)?;
    io.put_i32(col.data_type as i32)?;
    io.put_i32(col.option)?;
    io.put_i32(col.nrdim)?;

    // C++ writes shape for all non-scalar columns (if !isScalar_p)
    if col.is_array {
        write_iposition(io, &col.shape)?;
    }

    io.put_u32(col.max_length)?;
    write_table_record(io, &col.keywords)?;

    // Type-specific descriptor
    io.put_u32(1)?; // version=1
    if col.is_array {
        io.put_bool(false)?; // hasDefault=false
    } else {
        write_default_value(io, col.data_type)?;
    }

    Ok(())
}

pub(super) fn write_default_value(
    io: &mut AipsIo,
    dt: CasacoreDataType,
) -> Result<(), StorageError> {
    match dt {
        CasacoreDataType::TpBool => io.put_bool(false)?,
        CasacoreDataType::TpUChar => io.put_u8(0)?,
        CasacoreDataType::TpShort => io.put_i16(0)?,
        CasacoreDataType::TpUShort => io.put_u16(0)?,
        CasacoreDataType::TpInt => io.put_i32(0)?,
        CasacoreDataType::TpUInt => io.put_u32(0)?,
        CasacoreDataType::TpFloat => io.put_f32(0.0)?,
        CasacoreDataType::TpDouble => io.put_f64(0.0)?,
        CasacoreDataType::TpComplex => io.put_complex32(Complex32::new(0.0, 0.0))?,
        CasacoreDataType::TpDComplex => io.put_complex64(Complex64::new(0.0, 0.0))?,
        CasacoreDataType::TpString => io.put_string("")?,
        CasacoreDataType::TpInt64 => io.put_i64(0)?,
        // ScalarRecordColumnDesc has no default value.
        CasacoreDataType::TpRecord => {}
        _ => {
            return Err(StorageError::FormatMismatch(format!(
                "cannot write default for type {:?}",
                dt
            )));
        }
    }
    Ok(())
}

pub(super) fn write_column_set(
    io: &mut AipsIo,
    contents: &TableDatContents,
) -> Result<(), StorageError> {
    let cs = &contents.column_set;

    if let Some(storage) = cs.storage {
        io.put_i32(-3)?;
        io.put_u64(cs.nrrow)?;
        io.put_i32(storage.kind)?;
        io.put_i32(storage.block_size)?;
    } else {
        io.put_i32(-2)?;
        io.put_u32(cs.nrrow as u32)?;
    }
    io.put_u32(cs.seq_count)?;

    // Data managers
    io.put_u32(cs.data_managers.len() as u32)?;
    for dm in &cs.data_managers {
        io.put_string(&dm.type_name)?;
        io.put_u32(dm.seq_nr)?;
    }

    // PlainColumn entries
    for (i, col) in cs.columns.iter().enumerate() {
        io.put_u32(2)?; // version=2
        io.put_string(
            contents
                .names_on_file
                .get(&col.original_name)
                .unwrap_or(&col.original_name),
        )?;
        // ScaColData or ArrColData derived data
        io.put_u32(1)?; // version=1
        io.put_u32(col.dm_seq_nr)?;
        if col.is_array {
            // ArrColData: write shapeColDef + optional shapeCol (IPosition)
            let col_desc = &contents.table_desc.columns[i];
            let has_shape = col_desc.nrdim > 0 && !col_desc.shape.is_empty();
            io.put_bool(has_shape)?;
            if has_shape {
                write_iposition(io, &col_desc.shape)?;
            }
        }
    }

    // DM data blobs (empty for StManAipsIO, populated for StandardStMan)
    for dm in &cs.data_managers {
        io.put_u32(dm.data.len() as u32)?;
        if !dm.data.is_empty() {
            io.put_u8_slice(&dm.data, false)?;
        }
    }

    Ok(())
}

pub(crate) fn write_iposition(io: &mut AipsIo, values: &[i32]) -> Result<(), StorageError> {
    io.putstart("IPosition", 1)?;
    io.put_u32(values.len() as u32)?;
    io.put_i32_slice(values, false)?;
    io.putend()?;
    Ok(())
}

pub(super) fn write_table_record(
    io: &mut AipsIo,
    record: &RecordValue,
) -> Result<(), StorageError> {
    io.putstart("TableRecord", 1)?;
    write_record_desc(io, record)?;
    io.put_i32(1)?; // recordType = RecordInterface::Variable

    for field in record.fields() {
        write_record_field_value(io, &field.value)?;
    }

    io.putend()?;
    Ok(())
}

/// Deserialize a `RecordValue` from a uChar byte buffer (AipsIO `TableRecord` format).
///
/// C++ `ScalarRecordColumnData` stores records as `Vector<uChar>` containing
/// AipsIO-serialized `TableRecord` objects. This function reverses that encoding.
pub(crate) fn deserialize_record_from_uchar(bytes: &[u8]) -> Result<RecordValue, StorageError> {
    use casa_aipsio::ByteOrder;
    use std::io::Cursor;

    let mut io =
        AipsIo::new_read_only_with_order(Cursor::new(bytes.to_vec()), ByteOrder::BigEndian);
    read_table_record(&mut io)
}

/// Serialize a `RecordValue` to a uChar byte buffer (AipsIO `TableRecord` format).
///
/// Produces the same encoding that C++ `ScalarRecordColumnData::putRecord` writes:
/// a `TableRecord` AipsIO object serialized to a `Vector<uChar>`.
pub(crate) fn serialize_record_to_uchar(record: &RecordValue) -> Result<Vec<u8>, StorageError> {
    use casa_aipsio::ByteOrder;
    use std::io::Cursor;

    let buf = Vec::new();
    let mut io = AipsIo::new_write_only_with_order(Cursor::new(buf), ByteOrder::BigEndian);
    write_table_record(&mut io, record)?;
    let cursor: Cursor<Vec<u8>> = io
        .into_inner_typed()
        .map_err(|e| StorageError::FormatMismatch(format!("serialize_record_to_uchar: {e}")))?;
    Ok(cursor.into_inner())
}

pub(super) fn write_empty_record_desc(io: &mut AipsIo) -> Result<(), StorageError> {
    io.putstart("RecordDesc", 2)?;
    io.put_i32(0)?; // nfields=0
    io.putend()?;
    Ok(())
}

pub(super) fn write_record_desc(io: &mut AipsIo, record: &RecordValue) -> Result<(), StorageError> {
    io.putstart("RecordDesc", 2)?;
    io.put_i32(record.fields().len() as i32)?;

    for field in record.fields() {
        io.put_string(&field.name)?;
        let dt = value_to_casacore_data_type(&field.value)?;
        io.put_i32(dt as i32)?;

        if dt == CasacoreDataType::TpRecord {
            // C++ writes an empty sub-record schema here (nfields=0).
            // The actual fields are written in write_record_field_value as
            // a full TableRecord (Variable record type).
            write_empty_record_desc(io)?;
        } else if dt == CasacoreDataType::TpTable {
            // Table keyword entries carry an additional table-desc name slot.
            // We do not model named table descriptors, so persist an empty name
            // while keeping the wire layout aligned with casacore.
            io.put_string("")?;
        } else if is_array_data_type(dt) {
            // A keyword array is of variable shape, as `RecordDesc::addField`
            // without a shape makes it: `IPosition(1, -1)`. The array's own
            // shape is written with its data (`write_array_record_field`).
            // Writing the current shape here would fix the field to it, and
            // `TableRecordRep::defineDataField` then refuses a later update of
            // another shape (`Record::define - fixed array conformance error`).
            write_iposition(io, &[-1])?;
        }

        io.put_string("")?; // comment
    }

    io.putend()?;
    Ok(())
}

pub(super) fn write_record_field_value(io: &mut AipsIo, value: &Value) -> Result<(), StorageError> {
    match value {
        Value::Scalar(sv) => match sv {
            ScalarValue::Bool(v) => io.put_bool(*v)?,
            ScalarValue::UInt8(v) => io.put_u8(*v)?,
            ScalarValue::UInt16(v) => io.put_u16(*v)?,
            ScalarValue::UInt32(v) => io.put_u32(*v)?,
            ScalarValue::Int16(v) => io.put_i16(*v)?,
            ScalarValue::Int32(v) => io.put_i32(*v)?,
            ScalarValue::Int64(v) => io.put_i64(*v)?,
            ScalarValue::Float32(v) => io.put_f32(*v)?,
            ScalarValue::Float64(v) => io.put_f64(*v)?,
            ScalarValue::Complex32(v) => io.put_complex32(*v)?,
            ScalarValue::Complex64(v) => io.put_complex64(*v)?,
            ScalarValue::String(v) => io.put_string(v)?,
        },
        Value::TableRef(path) => io.put_string(path)?,
        Value::Record(record) => {
            write_table_record(io, record)?;
        }
        Value::Array(av) => {
            write_array_record_field(io, av)?;
        }
    }
    Ok(())
}

/// Write an array-typed record field to AipsIO.
///
/// C++ serializes `Array<T>` as: `"Array"` v3 → Int ndim → IPosition shape → count + data.
pub(super) fn write_array_record_field(
    io: &mut AipsIo,
    av: &ArrayValue,
) -> Result<(), StorageError> {
    // C++ RecordRep uses type-specific AipsIO names for arrays.
    let type_name = match av {
        ArrayValue::Bool(_) => "Array<void>",
        ArrayValue::UInt8(_) => "Array<uChar>",
        ArrayValue::Int16(_) => "Array<short>",
        ArrayValue::UInt16(_) => "Array<short>",
        ArrayValue::Int32(_) => "Array<Int>",
        ArrayValue::UInt32(_) => "Array<uInt>",
        ArrayValue::Int64(_) => "Array<Int64>",
        ArrayValue::Float32(_) => "Array<float>",
        ArrayValue::Float64(_) => "Array<double>",
        ArrayValue::Complex32(_) => "Array<void>",
        ArrayValue::Complex64(_) => "Array<void>",
        ArrayValue::String(_) => "Array<String>",
    };
    io.putstart(type_name, 3)?;

    // Write ndim as u32, then shape as ndim × u32 (NOT as IPosition).
    let ndim = av.ndim() as u32;
    io.put_u32(ndim)?;
    if ndim > 0 {
        for &d in av.shape() {
            io.put_u32(d as u32)?;
        }
        let nelem = av.len() as u32;
        io.put_u32(nelem)?;
        write_array_elements(io, av)?;
    }

    io.putend()?;
    Ok(())
}

/// Write an array's elements in casacore's order, the first axis fastest,
/// whatever the array's memory layout: an array held in C layout, or a
/// strided view, is written as the logical array it is, and an array in
/// Fortran layout is read straight from memory, as before. The reader
/// ([`read_array_record_field`]) rebuilds Fortran-layout arrays.
pub(super) fn write_array_elements(io: &mut AipsIo, av: &ArrayValue) -> Result<(), StorageError> {
    match av {
        ArrayValue::Bool(a) => {
            // casacore's TypeIO stores Booleans bit-packed, eight to a byte;
            // one byte each is a record casacore refuses ("part of object
            // not read").
            let flags: Vec<bool> = casacore_order(a).copied().collect();
            io.put_bool_slice(&flags, false)?;
            Ok(())
        }
        ArrayValue::UInt8(a) => put_each(io, a, AipsIo::put_u8),
        ArrayValue::Int16(a) => put_each(io, a, AipsIo::put_i16),
        ArrayValue::UInt16(a) => put_each(io, a, AipsIo::put_u16),
        ArrayValue::Int32(a) => put_each(io, a, AipsIo::put_i32),
        ArrayValue::UInt32(a) => put_each(io, a, AipsIo::put_u32),
        ArrayValue::Float32(a) => put_each(io, a, AipsIo::put_f32),
        ArrayValue::Float64(a) => put_each(io, a, AipsIo::put_f64),
        ArrayValue::Int64(a) => put_each(io, a, AipsIo::put_i64),
        ArrayValue::Complex32(a) => put_each(io, a, AipsIo::put_complex32),
        ArrayValue::Complex64(a) => put_each(io, a, AipsIo::put_complex64),
        ArrayValue::String(a) => {
            for s in casacore_order(a) {
                io.put_string(s)?;
            }
            Ok(())
        }
    }
}
