// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Read table.dat and dispatch based on the table type marker.
pub(crate) fn read_table_dat_dispatch(path: &Path) -> Result<TableDatResult, StorageError> {
    #[cfg(test)]
    CONTROL_FILE_READS.with(|reads| reads.set(reads.get() + 1));
    let mut io = AipsIo::open(path, AipsOpenOption::Old)?;
    let (table_version, nrrow, big_endian, table_type) = read_table_header(&mut io)?;
    match table_type.as_str() {
        "PlainTable" => {
            let table_desc = read_table_desc(&mut io)?;
            let ncol = table_desc.columns.len();
            let col_is_array: Vec<bool> = table_desc.columns.iter().map(|c| c.is_array).collect();
            let mut column_set = read_column_set(&mut io, table_version, ncol, &col_is_array)?;

            io.getend()?;
            io.close()?;

            // The column set lists its columns in the description's order;
            // a renamed column is listed under the name it had at creation.
            let mut names_on_file = std::collections::HashMap::new();
            for (entry, desc) in column_set.columns.iter_mut().zip(&table_desc.columns) {
                if entry.original_name != desc.col_name {
                    let on_file =
                        std::mem::replace(&mut entry.original_name, desc.col_name.clone());
                    names_on_file.insert(desc.col_name.clone(), on_file);
                }
            }

            let asdm = super::super::asdm_stman::Managers::from_bindings(
                path.parent().unwrap_or_else(|| Path::new(".")),
                &column_set.data_managers,
                nrrow.max(column_set.nrrow),
            );
            Ok(TableDatResult::Plain(TableDatContents {
                nrrow,
                big_endian,
                table_desc,
                column_set,
                names_on_file,
                asdm,
            }))
        }
        "RefTable" => {
            let ref_contents = read_ref_table_payload(&mut io, nrrow, big_endian)?;
            io.getend()?;
            io.close()?;
            Ok(TableDatResult::Ref(ref_contents))
        }
        "ConcatTable" => {
            let concat_contents = read_concat_table_payload(&mut io, nrrow, big_endian)?;
            io.getend()?;
            io.close()?;
            Ok(TableDatResult::Concat(concat_contents))
        }
        other => Err(StorageError::FormatMismatch(format!(
            "unsupported table type: {other}"
        ))),
    }
}

/// Read table.dat assuming it contains a PlainTable (backward-compat wrapper).
pub(crate) fn read_table_dat(path: &Path) -> Result<TableDatContents, StorageError> {
    match read_table_dat_dispatch(path)? {
        TableDatResult::Plain(contents) => Ok(contents),
        TableDatResult::Ref(_) => Err(StorageError::FormatMismatch(
            "expected PlainTable but found RefTable".to_string(),
        )),
        TableDatResult::Concat(_) => Err(StorageError::FormatMismatch(
            "expected PlainTable but found ConcatTable".to_string(),
        )),
    }
}

/// Read the AipsIO "RefTable" object payload from inside the "Table" envelope.
pub(crate) fn read_ref_table_file(
    file: std::fs::File,
) -> Result<RefTableDatContents, StorageError> {
    let mut io = AipsIo::new_read_only(file);
    let (_, rows, big_endian, kind) = read_table_header(&mut io)?;
    if kind != "RefTable" {
        return Err(StorageError::FormatMismatch(
            "expected a saved reference table".into(),
        ));
    }
    read_ref_table_payload(&mut io, rows, big_endian)
}

pub(super) fn read_ref_table_payload(
    io: &mut AipsIo,
    nrrow: u64,
    big_endian: bool,
) -> Result<RefTableDatContents, StorageError> {
    let version = io.getstart("RefTable")?;
    if version > 3 {
        return Err(StorageError::FormatMismatch(format!(
            "unsupported RefTable version: {version}"
        )));
    }

    // Parent table relative path.
    let parent_relative_path = io.get_string()?;

    // Column name map: C++ serializes std::map<String, String> as a
    // "SimpleOrderedMap" AipsIO object with a default value, count, old incr,
    // then (key, value) pairs.
    let _som_version = io.getstart("SimpleOrderedMap")?;
    let _default_value = io.get_string()?; // old default value (unused)
    let map_count = io.get_u32()? as usize;
    let _old_incr = io.get_u32()?; // old increment (unused)
    let mut column_name_map = Vec::with_capacity(map_count);
    for _ in 0..map_count {
        let view_name = io.get_string()?;
        let parent_name = io.get_string()?;
        column_name_map.push((view_name, parent_name));
    }
    io.getend()?;

    // Column names (version > 1 only).
    // C++ writes Vector<String> as an "Array" AipsIO object (version 3):
    //   ndim(u32), shape[ndim](u32...), count(u32), then strings.
    let column_names = if version > 1 {
        let _array_version = io.getstart("Array")?;
        let ndim = io.get_u32()?;
        let mut _shape = Vec::with_capacity(ndim as usize);
        for _ in 0..ndim {
            _shape.push(io.get_u32()?);
        }
        let n = io.get_u32()? as usize;
        let mut names = Vec::with_capacity(n);
        for _ in 0..n {
            names.push(io.get_string()?);
        }
        io.getend()?;
        names
    } else {
        // Version 1: derive from map keys.
        column_name_map.iter().map(|(k, _)| k.clone()).collect()
    };

    // Parent row count and row-order flag.
    let (parent_nrrow, row_order, ref_nrrow) = if version > 2 {
        let pnr = io.get_u64()?;
        let ro = io.get_bool()?;
        let rnr = io.get_u64()?;
        (pnr, ro, rnr)
    } else {
        let pnr = io.get_u32()? as u64;
        let ro = io.get_bool()?;
        let rnr = io.get_u32()? as u64;
        (pnr, ro, rnr)
    };

    // Row number vector (chunked at 2^20 per read, no count prefix).
    let max_chunk: u64 = 1 << 20;
    let mut row_map = vec![0u64; ref_nrrow as usize];
    let mut done: u64 = 0;
    while done < ref_nrrow {
        let todo = std::cmp::min(ref_nrrow - done, max_chunk) as usize;
        if version > 2 {
            io.get_u64_into(&mut row_map[done as usize..done as usize + todo])?;
        } else {
            let mut buf32 = vec![0u32; todo];
            io.get_u32_into(&mut buf32)?;
            for (i, &v) in buf32.iter().enumerate() {
                row_map[done as usize + i] = v as u64;
            }
        }
        done += todo as u64;
    }

    io.getend()?;

    Ok(RefTableDatContents {
        nrrow,
        big_endian,
        parent_relative_path,
        column_name_map,
        column_names,
        parent_nrrow,
        row_order,
        row_map,
    })
}

/// Read the AipsIO "ConcatTable" object payload from inside the "Table" envelope.
pub(super) fn read_concat_table_payload(
    io: &mut AipsIo,
    nrrow: u64,
    big_endian: bool,
) -> Result<ConcatTableDatContents, StorageError> {
    let version = io.getstart("ConcatTable")?;
    if version > 0 {
        return Err(StorageError::FormatMismatch(format!(
            "unsupported ConcatTable version: {version}"
        )));
    }

    // Number of constituent tables.
    let ntable = io.get_u32()? as usize;
    let mut table_paths = Vec::with_capacity(ntable);
    for _ in 0..ntable {
        table_paths.push(io.get_string()?);
    }

    // Block<String> subtable names: C++ serializes via operator<<(AipsIO, Block<String>)
    // which wraps in an AipsIO "Block" object envelope (version 1).
    let _block_version = io.getstart("Block")?;
    let nsub = io.get_u32()? as usize;
    let mut sub_table_names = Vec::with_capacity(nsub);
    for _ in 0..nsub {
        sub_table_names.push(io.get_string()?);
    }
    io.getend()?; // Block

    io.getend()?; // ConcatTable

    Ok(ConcatTableDatContents {
        nrrow,
        big_endian,
        table_paths,
        sub_table_names,
    })
}

pub(super) fn read_table_desc(io: &mut AipsIo) -> Result<TableDescContents, StorageError> {
    let version = io.getstart("TableDesc")?;

    let name = io.get_string()?;
    let vers = io.get_string()?;
    let comment = io.get_string()?;

    let table_keywords = read_table_record(io)?;

    let private_keywords = if version >= 2 {
        read_table_record(io)?
    } else {
        RecordValue::default()
    };

    // ColumnDescSet: read ncol then each column
    let ncol = io.get_u32()?;
    let mut columns = Vec::with_capacity(ncol as usize);
    for _ in 0..ncol {
        columns.push(read_column_desc(io)?);
    }

    io.getend()?;

    Ok(TableDescContents {
        name,
        version: vers,
        comment,
        table_keywords,
        private_keywords,
        columns,
    })
}

pub(super) fn read_column_desc(io: &mut AipsIo) -> Result<ColumnDescContents, StorageError> {
    // ColumnDesc wrapper
    let _col_desc_version = io.get_u32()?; // version=1
    let class_name = io.get_string()?;

    let class_info = parse_column_class_name(&class_name).ok_or_else(|| {
        StorageError::FormatMismatch(format!("unknown column class name: {class_name}"))
    })?;

    let (primitive_type, is_array) = match class_info {
        ColumnClassInfo::Typed {
            primitive_type,
            is_array,
        } => (Some(primitive_type), is_array),
        ColumnClassInfo::Record => (None, false),
    };

    // BaseColumnDesc
    let _base_version = io.get_u32()?; // version=1
    let col_name = io.get_string()?;
    let comment = io.get_string()?;
    let data_manager_type = io.get_string()?;
    let data_manager_group = io.get_string()?;
    let data_type_i32 = io.get_i32()?;
    let data_type = CasacoreDataType::from_i32(data_type_i32).ok_or_else(|| {
        StorageError::FormatMismatch(format!("unknown data type: {data_type_i32}"))
    })?;
    let option = io.get_i32()?;
    let nrdim = io.get_i32()?;

    // C++ writes shape for ALL non-scalar columns (if !isScalar_p),
    // regardless of nrdim. This includes array columns with nrdim=0.
    let shape = if is_array {
        read_iposition(io)?
    } else {
        vec![]
    };

    let max_length = io.get_u32()?;

    let keywords = read_table_record(io)?;

    // Type-specific descriptor (ScalarColumnDesc or ArrayColumnDesc)
    let _type_version = io.get_u32()?; // version=1
    if is_array {
        // ArrayColumnDesc: hasDefault (Bool)
        let _has_default = io.get_bool()?;
    } else {
        // ScalarColumnDesc: default value
        skip_default_value(io, data_type)?;
    }

    Ok(ColumnDescContents {
        class_name,
        col_name,
        comment,
        data_manager_type,
        data_manager_group,
        data_type,
        option,
        nrdim,
        shape,
        max_length,
        keywords,
        is_array,
        primitive_type,
    })
}

pub(super) fn skip_default_value(
    io: &mut AipsIo,
    dt: CasacoreDataType,
) -> Result<(), StorageError> {
    match dt {
        CasacoreDataType::TpBool => {
            io.get_bool()?;
        }
        CasacoreDataType::TpUChar => {
            io.get_u8()?;
        }
        CasacoreDataType::TpShort => {
            io.get_i16()?;
        }
        CasacoreDataType::TpUShort => {
            io.get_u16()?;
        }
        CasacoreDataType::TpInt => {
            io.get_i32()?;
        }
        CasacoreDataType::TpUInt => {
            io.get_u32()?;
        }
        CasacoreDataType::TpFloat => {
            io.get_f32()?;
        }
        CasacoreDataType::TpDouble => {
            io.get_f64()?;
        }
        CasacoreDataType::TpComplex => {
            io.get_complex32()?;
        }
        CasacoreDataType::TpDComplex => {
            io.get_complex64()?;
        }
        CasacoreDataType::TpString => {
            io.get_string()?;
        }
        CasacoreDataType::TpInt64 => {
            io.get_i64()?;
        }
        // ScalarRecordColumnDesc has no default value.
        CasacoreDataType::TpRecord => {}
        _ => {
            return Err(StorageError::FormatMismatch(format!(
                "cannot skip default value for type {:?}",
                dt
            )));
        }
    }
    Ok(())
}

pub(super) fn read_column_set(
    io: &mut AipsIo,
    _table_version: u32,
    ncol: usize,
    col_is_array: &[bool],
) -> Result<ColumnSetContents, StorageError> {
    let version = io.get_i32()?; // negative: -2 or -3

    let (nrrow, storage) = if version == -3 {
        let nr = io.get_u64()?;
        let kind = io.get_i32()?;
        let block_size = io.get_i32()?;
        (nr, Some(ColumnSetStorage { kind, block_size }))
    } else {
        (u64::from(io.get_u32()?), None)
    };

    let seq_count = io.get_u32()?;

    let num_dm = io.get_u32()?;
    let mut data_managers = Vec::with_capacity(num_dm as usize);
    for _ in 0..num_dm {
        let type_name = io.get_string()?;
        let seq_nr = io.get_u32()?;
        data_managers.push(DataManagerEntry {
            type_name,
            seq_nr,
            data: Vec::new(),
        });
    }

    // Read PlainColumn entries (one per column in the table)
    let mut columns = Vec::with_capacity(ncol);
    for &is_array in &col_is_array[..ncol] {
        columns.push(read_plain_column(io, is_array)?);
    }

    // Read DM data blobs (one per data manager) — preserve for StandardStMan
    for dm in &mut data_managers {
        let len = io.get_u32()?;
        if len > 0 {
            let mut buf = vec![0u8; len as usize];
            io.get_u8_into(&mut buf)?;
            dm.data = buf;
        }
    }

    Ok(ColumnSetContents {
        nrrow,
        storage,
        seq_count,
        data_managers,
        columns,
    })
}

pub(super) fn read_plain_column(
    io: &mut AipsIo,
    is_array: bool,
) -> Result<PlainColumnEntry, StorageError> {
    let _version = io.get_u32()?; // version=2
    let original_name = io.get_string()?;

    // Type-specific derived data (ScaColData or ArrColData)
    let _derived_version = io.get_u32()?; // version=1
    let dm_seq_nr = io.get_u32()?;

    if is_array {
        // ArrColData: shapeColDef (bool) + optional shapeCol (IPosition)
        let shape_col_def = io.get_bool()?;
        if shape_col_def {
            // C++ writes IPosition via ios << shapeCol_p (AipsIO object, not string)
            let _shape_col = read_iposition(io)?;
        }
    }

    Ok(PlainColumnEntry {
        original_name,
        dm_seq_nr,
        is_array,
    })
}

// ---------------------------------------------------------------------------
// IPosition helpers
// ---------------------------------------------------------------------------

pub(crate) fn read_iposition(io: &mut AipsIo) -> Result<Vec<i32>, StorageError> {
    let _version = io.getstart("IPosition")?;
    let n = io.get_u32()?;
    let mut values = vec![0i32; n as usize];
    io.get_i32_into(&mut values)?;
    io.getend()?;
    Ok(values)
}

// ---------------------------------------------------------------------------
// TableRecord / RecordDesc helpers
// ---------------------------------------------------------------------------

/// Read a C++ `Record` (serialized as `"RecordRep"`) from AipsIO.
///
/// This is the format used by `TSMCube::values_p` in tiled storage managers.
/// Structurally identical to `TableRecord` but uses a different AipsIO type name.
pub(crate) fn read_record_rep(io: &mut AipsIo) -> Result<RecordValue, StorageError> {
    let _version = io.getstart("RecordRep")?;
    let desc = read_record_desc(io)?;
    let _record_type = io.get_i32()?;
    let mut fields = Vec::with_capacity(desc.len());
    for (name, field_type) in &desc {
        let value = read_record_field_value(io, *field_type)?;
        fields.push(RecordField::new(name.clone(), value));
    }
    io.getend()?;
    Ok(RecordValue::new(fields))
}

/// Read a C++ `Record` object.
///
/// C++ `RecordRep::putRecord` writes a single `"Record"` v1 envelope containing
/// `RecordDesc`, `recordType` (Int), then the raw field data. There is no nested
/// `"RecordRep"` envelope — that name is only used in `TableRecord` context.
///
/// The TSMCube `values` field uses this format (`ios << values_p` in TSMCube.cc).
pub(crate) fn read_record(io: &mut AipsIo) -> Result<RecordValue, StorageError> {
    let _version = io.getstart("Record")?;
    let desc = read_record_desc(io)?;
    let _record_type = io.get_i32()?;
    let mut fields = Vec::with_capacity(desc.len());
    for (name, field_type) in &desc {
        let value = read_record_field_value(io, *field_type)?;
        fields.push(RecordField::new(name.clone(), value));
    }
    io.getend()?;
    Ok(RecordValue::new(fields))
}

/// Write a C++ `Record` object.
///
/// Writes: `putstart("Record", 1)`, `RecordDesc`, `recordType`, field data, `putend()`.
/// This matches C++ `RecordRep::putRecord`.
pub(crate) fn write_record(io: &mut AipsIo, record: &RecordValue) -> Result<(), StorageError> {
    io.putstart("Record", 1)?;
    write_record_desc(io, record)?;
    io.put_i32(1)?; // recordType = RecordInterface::Variable
    for field in record.fields() {
        write_record_field_value(io, &field.value)?;
    }
    io.putend()?;
    Ok(())
}

/// Write a C++ `Record` (serialized as `"RecordRep"`) to AipsIO.
pub(crate) fn write_record_rep(io: &mut AipsIo, record: &RecordValue) -> Result<(), StorageError> {
    io.putstart("RecordRep", 1)?;
    write_record_desc(io, record)?;
    io.put_i32(1)?; // recordType = RecordInterface::Variable
    for field in record.fields() {
        write_record_field_value(io, &field.value)?;
    }
    io.putend()?;
    Ok(())
}

/// Read a standalone casacore `TableRecord`, such as a saved image region.
///
/// Returns an error if the file cannot be opened or decoded as a `TableRecord`.
pub fn read_table_record_file(path: &Path) -> Result<RecordValue, StorageError> {
    let mut io = AipsIo::open(path, AipsOpenOption::Old)?;
    read_table_record(&mut io)
}

pub(super) fn read_table_record(io: &mut AipsIo) -> Result<RecordValue, StorageError> {
    let _version = io.getstart("TableRecord")?;

    let desc = read_record_desc(io)?;
    let _record_type = io.get_i32()?; // RecordType enum

    let mut fields = Vec::with_capacity(desc.len());
    for (name, field_type) in &desc {
        let value = read_record_field_value(io, *field_type)?;
        fields.push(RecordField::new(name.clone(), value));
    }

    io.getend()?;
    Ok(RecordValue::new(fields))
}

pub(super) fn read_record_desc(
    io: &mut AipsIo,
) -> Result<Vec<(String, CasacoreDataType)>, StorageError> {
    let _version = io.getstart("RecordDesc")?;
    let nfields = io.get_i32()?;
    if nfields < 0 {
        return Err(StorageError::FormatMismatch(format!(
            "negative RecordDesc field count: {nfields}"
        )));
    }
    let mut desc = Vec::new();

    for _ in 0..nfields {
        let name = io.get_string()?;
        let type_i32 = io.get_i32()?;
        let dt = CasacoreDataType::from_i32(type_i32).ok_or_else(|| {
            StorageError::FormatMismatch(format!("unknown RecordDesc field type: {type_i32}"))
        })?;

        // Sub-record, array shape, or table name depending on type
        if dt == CasacoreDataType::TpRecord {
            // Recursive RecordDesc — skip it (we don't use sub-record shapes)
            let _sub_desc = read_record_desc(io)?;
        } else if is_array_data_type(dt) {
            // Array field: read IPosition shape
            let _shape = read_iposition(io)?;
        } else if dt == CasacoreDataType::TpTable {
            let _table_desc_name = io.get_string()?;
        }

        let _comment = io.get_string()?;
        desc.push((name, dt));
    }

    io.getend()?;
    Ok(desc)
}

pub(super) fn read_record_field_value(
    io: &mut AipsIo,
    dt: CasacoreDataType,
) -> Result<Value, StorageError> {
    match dt {
        CasacoreDataType::TpBool => Ok(Value::Scalar(ScalarValue::Bool(io.get_bool()?))),
        CasacoreDataType::TpUChar => Ok(Value::Scalar(ScalarValue::UInt8(io.get_u8()?))),
        CasacoreDataType::TpShort => Ok(Value::Scalar(ScalarValue::Int16(io.get_i16()?))),
        CasacoreDataType::TpUShort => Ok(Value::Scalar(ScalarValue::UInt16(io.get_u16()?))),
        CasacoreDataType::TpInt => Ok(Value::Scalar(ScalarValue::Int32(io.get_i32()?))),
        CasacoreDataType::TpUInt => Ok(Value::Scalar(ScalarValue::UInt32(io.get_u32()?))),
        CasacoreDataType::TpFloat => Ok(Value::Scalar(ScalarValue::Float32(io.get_f32()?))),
        CasacoreDataType::TpDouble => Ok(Value::Scalar(ScalarValue::Float64(io.get_f64()?))),
        CasacoreDataType::TpComplex => {
            Ok(Value::Scalar(ScalarValue::Complex32(io.get_complex32()?)))
        }
        CasacoreDataType::TpDComplex => {
            Ok(Value::Scalar(ScalarValue::Complex64(io.get_complex64()?)))
        }
        CasacoreDataType::TpString => Ok(Value::Scalar(ScalarValue::String(io.get_string()?))),
        CasacoreDataType::TpInt64 => Ok(Value::Scalar(ScalarValue::Int64(io.get_i64()?))),
        CasacoreDataType::TpRecord => {
            let record = read_table_record(io)?;
            Ok(Value::Record(record))
        }
        // Table reference keyword — stores a subtable path as a string.
        // C++ casacore writes these via `TableKeywordSet::toRecord()` and
        // the `TableAttr` class. We represent them as plain string values
        // so the keyword name→path mapping is preserved.
        CasacoreDataType::TpTable => Ok(Value::TableRef(io.get_string()?)),
        // Array types — used in hypercolumn definitions and cube metadata.
        CasacoreDataType::TpArrayBool
        | CasacoreDataType::TpArrayUChar
        | CasacoreDataType::TpArrayShort
        | CasacoreDataType::TpArrayUShort
        | CasacoreDataType::TpArrayInt
        | CasacoreDataType::TpArrayUInt
        | CasacoreDataType::TpArrayFloat
        | CasacoreDataType::TpArrayDouble
        | CasacoreDataType::TpArrayComplex
        | CasacoreDataType::TpArrayDComplex
        | CasacoreDataType::TpArrayString
        | CasacoreDataType::TpArrayInt64 => read_array_record_field(io, dt),
        _ => Err(StorageError::FormatMismatch(format!(
            "unsupported record field type: {dt:?}"
        ))),
    }
}

/// Read an array-typed record field from AipsIO.
///
/// C++ RecordRep uses type-specific names: `"Array<String>"`, `"Array<float>"`, etc.
/// The reader peeks the actual type name and uses it for `getstart`.
pub(super) fn read_array_record_field(
    io: &mut AipsIo,
    dt: CasacoreDataType,
) -> Result<Value, StorageError> {
    // C++ writes different type names depending on the element type.
    // Peek the actual type name and use it for getstart.
    let type_name = io.get_next_type()?;
    let version = io.getstart(&type_name)?;
    let ndim = io.get_u32()? as usize;

    // Older versions (< 3) include an origin before the shape.
    if version < 3 {
        for _ in 0..ndim {
            let _origin = io.get_i32()?;
        }
    }

    // Shape: ndim × u32 (NOT an IPosition AipsIO object).
    let mut shape = Vec::with_capacity(ndim.max(1));
    for _ in 0..ndim {
        shape.push(io.get_u32()? as usize);
    }
    let count = io.get_u32()? as usize;
    if ndim == 0 {
        // casacore's empty IPosition has no elements. A rank-zero ndarray
        // would instead hold one element, so represent this as shape [0].
        if count != 0 {
            return Err(StorageError::FormatMismatch(format!(
                "rank-zero record array has element count {count}, expected 0"
            )));
        }
        shape.push(0);
    }
    let count = count.min(shape.iter().product());

    let av = match dt {
        CasacoreDataType::TpArrayBool => {
            let mut vals = vec![false; count];
            // casacore's TypeIO stores Booleans bit-packed, eight to a byte.
            io.get_bool_into(&mut vals)?;
            ArrayValue::Bool(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpArrayUChar => {
            let mut vals = vec![0u8; count];
            for v in vals.iter_mut() {
                *v = io.get_u8()?;
            }
            ArrayValue::UInt8(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpArrayShort => {
            let mut vals = vec![0i16; count];
            for v in vals.iter_mut() {
                *v = io.get_i16()?;
            }
            ArrayValue::Int16(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpArrayUShort => {
            let mut vals = vec![0u16; count];
            for v in vals.iter_mut() {
                *v = io.get_u16()?;
            }
            ArrayValue::UInt16(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpArrayInt => {
            let mut vals = vec![0i32; count];
            for v in vals.iter_mut() {
                *v = io.get_i32()?;
            }
            ArrayValue::Int32(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpArrayUInt => {
            let mut vals = vec![0u32; count];
            for v in vals.iter_mut() {
                *v = io.get_u32()?;
            }
            ArrayValue::UInt32(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpArrayFloat => {
            let mut vals = vec![0f32; count];
            for v in vals.iter_mut() {
                *v = io.get_f32()?;
            }
            ArrayValue::Float32(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpArrayDouble => {
            let mut vals = vec![0f64; count];
            for v in vals.iter_mut() {
                *v = io.get_f64()?;
            }
            ArrayValue::Float64(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpArrayComplex => {
            let mut vals = vec![Complex32::new(0.0, 0.0); count];
            for v in vals.iter_mut() {
                *v = io.get_complex32()?;
            }
            ArrayValue::Complex32(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpArrayDComplex => {
            let mut vals = vec![Complex64::new(0.0, 0.0); count];
            for v in vals.iter_mut() {
                *v = io.get_complex64()?;
            }
            ArrayValue::Complex64(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpArrayString => {
            let mut vals = Vec::with_capacity(count);
            for _ in 0..count {
                vals.push(io.get_string()?);
            }
            ArrayValue::String(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpArrayInt64 => {
            let mut vals = vec![0i64; count];
            for v in vals.iter_mut() {
                *v = io.get_i64()?;
            }
            ArrayValue::Int64(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        _ => {
            return Err(StorageError::FormatMismatch(format!(
                "unsupported array record field type: {dt:?}"
            )));
        }
    };

    io.getend()?;
    Ok(Value::Array(av))
}
