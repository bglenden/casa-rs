// SPDX-License-Identifier: LGPL-3.0-or-later
#![allow(dead_code)]

use std::path::Path;

use casacore_aipsio::{AipsIo, AipsOpenOption};
use casacore_types::{
    Complex32, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};

use super::StorageError;
use super::data_type::{
    CasacoreDataType, array_column_class_name, parse_column_class_name, scalar_column_class_name,
};

// ---------------------------------------------------------------------------
// Parsed structures
// ---------------------------------------------------------------------------

/// Result of reading table.dat — dispatches on the table type marker.
#[derive(Debug, Clone)]
pub(crate) enum TableDatResult {
    Plain(TableDatContents),
    Ref(RefTableDatContents),
}

/// Contents of a RefTable's table.dat (no TableDesc/ColumnSet — just
/// the parent path, column name map, and row number vector).
#[derive(Debug, Clone)]
pub(crate) struct RefTableDatContents {
    pub nrrow: u64,
    pub big_endian: bool,
    /// Relative path from this table's directory to the root (parent) table.
    pub parent_relative_path: String,
    /// Column name mapping: (view_name, parent_name) pairs.
    pub column_name_map: Vec<(String, String)>,
    /// Ordered column names in this view.
    pub column_names: Vec<String>,
    /// Row count of the parent table at the time this RefTable was saved.
    pub parent_nrrow: u64,
    /// True if row numbers are in ascending order.
    pub row_order: bool,
    /// Maps RefTable row i → parent row row_map[i].
    pub row_map: Vec<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct TableDatContents {
    pub nrrow: u64,
    pub big_endian: bool,
    pub table_desc: TableDescContents,
    pub column_set: ColumnSetContents,
}

#[derive(Debug, Clone)]
pub(crate) struct TableDescContents {
    pub name: String,
    pub version: String,
    pub comment: String,
    pub table_keywords: RecordValue,
    pub private_keywords: RecordValue,
    pub columns: Vec<ColumnDescContents>,
}

#[derive(Debug, Clone)]
pub(crate) struct ColumnDescContents {
    pub class_name: String,
    pub col_name: String,
    pub comment: String,
    pub data_manager_type: String,
    pub data_manager_group: String,
    pub data_type: CasacoreDataType,
    pub option: i32,
    pub nrdim: i32,
    pub shape: Vec<i32>,
    pub max_length: u32,
    pub keywords: RecordValue,
    pub is_array: bool,
    pub primitive_type: PrimitiveType,
}

#[derive(Debug, Clone)]
pub(crate) struct ColumnSetContents {
    pub nrrow: u64,
    pub seq_count: u32,
    pub data_managers: Vec<DataManagerEntry>,
    pub columns: Vec<PlainColumnEntry>,
}

#[derive(Debug, Clone)]
pub(crate) struct DataManagerEntry {
    pub type_name: String,
    pub seq_nr: u32,
    /// Raw data blob from the ColumnSet section of table.dat.
    /// Empty for StManAipsIO; contains SSM header info for StandardStMan.
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct PlainColumnEntry {
    pub original_name: String,
    pub dm_seq_nr: u32,
    pub is_array: bool,
}

// ---------------------------------------------------------------------------
// Read path
// ---------------------------------------------------------------------------

/// Read table.dat and dispatch based on the table type marker.
pub(crate) fn read_table_dat_dispatch(path: &Path) -> Result<TableDatResult, StorageError> {
    let mut io = AipsIo::open(path, AipsOpenOption::Old)?;
    let table_version = io.getstart("Table")?;

    let nrrow = if table_version >= 3 {
        io.get_u64()?
    } else {
        io.get_u32()? as u64
    };

    let format = io.get_u32()?;
    let big_endian = format == 0;

    let table_type = io.get_string()?;

    match table_type.as_str() {
        "PlainTable" => {
            let table_desc = read_table_desc(&mut io)?;
            let ncol = table_desc.columns.len();
            let col_is_array: Vec<bool> = table_desc.columns.iter().map(|c| c.is_array).collect();
            let column_set = read_column_set(&mut io, table_version, ncol, &col_is_array)?;

            io.getend()?;
            io.close()?;

            Ok(TableDatResult::Plain(TableDatContents {
                nrrow,
                big_endian,
                table_desc,
                column_set,
            }))
        }
        "RefTable" => {
            let ref_contents = read_ref_table_payload(&mut io, nrrow, big_endian)?;
            io.getend()?;
            io.close()?;
            Ok(TableDatResult::Ref(ref_contents))
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
    }
}

/// Read the AipsIO "RefTable" object payload from inside the "Table" envelope.
fn read_ref_table_payload(
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

fn read_table_desc(io: &mut AipsIo) -> Result<TableDescContents, StorageError> {
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

fn read_column_desc(io: &mut AipsIo) -> Result<ColumnDescContents, StorageError> {
    // ColumnDesc wrapper
    let _col_desc_version = io.get_u32()?; // version=1
    let class_name = io.get_string()?;

    let (primitive_type, is_array) = parse_column_class_name(&class_name).ok_or_else(|| {
        StorageError::FormatMismatch(format!("unknown column class name: {class_name}"))
    })?;

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

fn skip_default_value(io: &mut AipsIo, dt: CasacoreDataType) -> Result<(), StorageError> {
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
        _ => {
            return Err(StorageError::FormatMismatch(format!(
                "cannot skip default value for type {:?}",
                dt
            )));
        }
    }
    Ok(())
}

fn read_column_set(
    io: &mut AipsIo,
    _table_version: u32,
    ncol: usize,
    col_is_array: &[bool],
) -> Result<ColumnSetContents, StorageError> {
    let version = io.get_i32()?; // negative: -2 or -3

    let nrrow = if version == -3 {
        let nr = io.get_u64()?;
        let _storage_opt = io.get_i32()?;
        let _block_size = io.get_i32()?;
        nr
    } else {
        io.get_u32()? as u64
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
        seq_count,
        data_managers,
        columns,
    })
}

fn read_plain_column(io: &mut AipsIo, is_array: bool) -> Result<PlainColumnEntry, StorageError> {
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

fn read_iposition(io: &mut AipsIo) -> Result<Vec<i32>, StorageError> {
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

fn read_table_record(io: &mut AipsIo) -> Result<RecordValue, StorageError> {
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

fn read_record_desc(io: &mut AipsIo) -> Result<Vec<(String, CasacoreDataType)>, StorageError> {
    let _version = io.getstart("RecordDesc")?;
    let nfields = io.get_i32()?;
    let mut desc = Vec::with_capacity(nfields as usize);

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

fn read_record_field_value(io: &mut AipsIo, dt: CasacoreDataType) -> Result<Value, StorageError> {
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
        _ => Err(StorageError::FormatMismatch(format!(
            "unsupported record field type: {dt:?}"
        ))),
    }
}

fn is_array_data_type(dt: CasacoreDataType) -> bool {
    matches!(
        dt,
        CasacoreDataType::TpArrayBool
            | CasacoreDataType::TpArrayChar
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
            | CasacoreDataType::TpArrayInt64
    )
}

// ---------------------------------------------------------------------------
// Write path
// ---------------------------------------------------------------------------

pub(crate) fn write_table_dat(
    path: &Path,
    contents: &TableDatContents,
) -> Result<(), StorageError> {
    let mut io = AipsIo::open(path, AipsOpenOption::New)?;

    // Write Table version 2 (nrrow as u32)
    io.putstart("Table", 2)?;
    io.put_u32(contents.nrrow as u32)?;
    io.put_u32(if contents.big_endian { 0 } else { 1 })?;
    io.put_string("PlainTable")?;

    write_table_desc(&mut io, &contents.table_desc)?;
    write_column_set(&mut io, contents)?;

    io.putend()?;
    io.close()?;
    Ok(())
}

/// Write a RefTable table.dat file.
pub(crate) fn write_ref_table_dat(
    path: &Path,
    contents: &RefTableDatContents,
) -> Result<(), StorageError> {
    let mut io = AipsIo::open(path, AipsOpenOption::New)?;

    // Table envelope — version 2 (nrrow as u32).
    io.putstart("Table", 2)?;
    io.put_u32(contents.nrrow as u32)?;
    io.put_u32(if contents.big_endian { 0 } else { 1 })?;
    io.put_string("RefTable")?;

    // RefTable payload — version 2 (32-bit row numbers).
    io.putstart("RefTable", 2)?;

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

    // Parent row count at save time.
    io.put_u32(contents.parent_nrrow as u32)?;

    // Row order flag.
    io.put_bool(contents.row_order)?;

    // This RefTable's row count.
    io.put_u32(contents.nrrow as u32)?;

    // Row number vector (chunked at 2^20, no count prefix).
    let max_chunk: usize = 1 << 20;
    let mut done: usize = 0;
    let total = contents.row_map.len();
    while done < total {
        let todo = std::cmp::min(total - done, max_chunk);
        let chunk32: Vec<u32> = contents.row_map[done..done + todo]
            .iter()
            .map(|&r| r as u32)
            .collect();
        io.put_u32_slice(&chunk32, false)?;
        done += todo;
    }

    io.putend()?; // RefTable

    io.putend()?; // Table
    io.close()?;
    Ok(())
}

fn write_table_desc(io: &mut AipsIo, desc: &TableDescContents) -> Result<(), StorageError> {
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

fn write_column_desc(io: &mut AipsIo, col: &ColumnDescContents) -> Result<(), StorageError> {
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

fn write_default_value(io: &mut AipsIo, dt: CasacoreDataType) -> Result<(), StorageError> {
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
        _ => {
            return Err(StorageError::FormatMismatch(format!(
                "cannot write default for type {:?}",
                dt
            )));
        }
    }
    Ok(())
}

fn write_column_set(io: &mut AipsIo, contents: &TableDatContents) -> Result<(), StorageError> {
    let cs = &contents.column_set;

    // Version -2 (nrrow as u32)
    io.put_i32(-2)?;
    io.put_u32(cs.nrrow as u32)?;
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
        io.put_string(&col.original_name)?;
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

fn write_iposition(io: &mut AipsIo, values: &[i32]) -> Result<(), StorageError> {
    io.putstart("IPosition", 1)?;
    io.put_u32(values.len() as u32)?;
    io.put_i32_slice(values, false)?;
    io.putend()?;
    Ok(())
}

fn write_table_record(io: &mut AipsIo, record: &RecordValue) -> Result<(), StorageError> {
    io.putstart("TableRecord", 1)?;
    write_record_desc(io, record)?;
    io.put_i32(1)?; // recordType = RecordInterface::Variable

    for field in record.fields() {
        write_record_field_value(io, &field.value)?;
    }

    io.putend()?;
    Ok(())
}

fn write_record_desc(io: &mut AipsIo, record: &RecordValue) -> Result<(), StorageError> {
    io.putstart("RecordDesc", 2)?;
    io.put_i32(record.fields().len() as i32)?;

    for field in record.fields() {
        io.put_string(&field.name)?;
        let dt = value_to_casacore_data_type(&field.value)?;
        io.put_i32(dt as i32)?;

        if dt == CasacoreDataType::TpRecord {
            if let Value::Record(sub) = &field.value {
                write_record_desc(io, sub)?;
            }
        }

        io.put_string("")?; // comment
    }

    io.putend()?;
    Ok(())
}

fn write_record_field_value(io: &mut AipsIo, value: &Value) -> Result<(), StorageError> {
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
        Value::Record(record) => {
            write_table_record(io, record)?;
        }
        Value::Array(_) => {
            return Err(StorageError::FormatMismatch(
                "array values in keywords not yet supported".to_string(),
            ));
        }
    }
    Ok(())
}

fn value_to_casacore_data_type(value: &Value) -> Result<CasacoreDataType, StorageError> {
    match value {
        Value::Scalar(sv) => {
            let pt = sv.primitive_type();
            Ok(CasacoreDataType::from_primitive_type(pt, false))
        }
        Value::Record(_) => Ok(CasacoreDataType::TpRecord),
        Value::Array(av) => {
            let pt = av.primitive_type();
            Ok(CasacoreDataType::from_primitive_type(pt, true))
        }
    }
}

// ---------------------------------------------------------------------------
// Conversion helpers: TableDatContents <-> schema types
// ---------------------------------------------------------------------------

use crate::schema::{ColumnSchema, TableSchema};

impl TableDatContents {
    pub(crate) fn to_table_schema(&self) -> Result<TableSchema, StorageError> {
        let columns: Vec<ColumnSchema> = self
            .table_desc
            .columns
            .iter()
            .map(|c| c.to_column_schema())
            .collect::<Result<_, _>>()?;
        Ok(TableSchema::new(columns)?)
    }

    pub(crate) fn from_snapshot(
        schema: &TableSchema,
        keywords: &RecordValue,
        column_keywords: &std::collections::HashMap<String, RecordValue>,
        nrrow: u64,
        dm_type_name: &str,
        dm_data: &[u8],
        big_endian: bool,
    ) -> Self {
        let columns: Vec<ColumnDescContents> = schema
            .columns()
            .iter()
            .map(|col| {
                let mut desc = ColumnDescContents::from_column_schema(col);
                if let Some(kw) = column_keywords.get(col.name()) {
                    desc.keywords = kw.clone();
                }
                desc
            })
            .collect();

        let plain_columns: Vec<PlainColumnEntry> = columns
            .iter()
            .map(|c| PlainColumnEntry {
                original_name: c.col_name.clone(),
                dm_seq_nr: 0,
                is_array: c.is_array,
            })
            .collect();

        TableDatContents {
            nrrow,
            big_endian,
            table_desc: TableDescContents {
                name: String::new(),
                version: String::new(),
                comment: String::new(),
                table_keywords: keywords.clone(),
                private_keywords: RecordValue::default(),
                columns,
            },
            column_set: ColumnSetContents {
                nrrow,
                seq_count: 1,
                data_managers: vec![DataManagerEntry {
                    type_name: dm_type_name.to_string(),
                    seq_nr: 0,
                    data: dm_data.to_vec(),
                }],
                columns: plain_columns,
            },
        }
    }
}

impl ColumnDescContents {
    fn to_column_schema(&self) -> Result<ColumnSchema, StorageError> {
        if self.is_array {
            if self.nrdim > 0 && !self.shape.is_empty() {
                let shape: Vec<usize> = self.shape.iter().map(|&s| s as usize).collect();
                Ok(ColumnSchema::array_fixed(
                    &self.col_name,
                    self.primitive_type,
                    shape,
                ))
            } else if self.nrdim > 0 {
                Ok(ColumnSchema::array_variable(
                    &self.col_name,
                    self.primitive_type,
                    Some(self.nrdim as usize),
                ))
            } else {
                Ok(ColumnSchema::array_variable(
                    &self.col_name,
                    self.primitive_type,
                    None,
                ))
            }
        } else {
            Ok(ColumnSchema::scalar(&self.col_name, self.primitive_type))
        }
    }

    fn from_column_schema(col: &ColumnSchema) -> Self {
        use crate::schema::{ArrayShapeContract, ColumnType};

        let pt = col.data_type().unwrap_or(PrimitiveType::Int32);
        let is_array = matches!(col.column_type(), ColumnType::Array(_));

        let (nrdim, shape) = match col.column_type() {
            ColumnType::Array(ArrayShapeContract::Fixed { shape }) => {
                let s: Vec<i32> = shape.iter().map(|&d| d as i32).collect();
                (s.len() as i32, s)
            }
            ColumnType::Array(ArrayShapeContract::Variable { ndim: Some(n) }) => {
                (*n as i32, vec![])
            }
            _ => (0, vec![]),
        };

        let class_name = if is_array {
            array_column_class_name(pt).to_string()
        } else {
            scalar_column_class_name(pt).to_string()
        };

        // C++ casacore ColumnDesc stores the scalar element type (e.g. TpFloat)
        // in the data_type field, even for array columns. The array-ness is
        // conveyed by the className (ArrayColumnDesc<T>).
        let dt = CasacoreDataType::from_primitive_type(pt, false);

        // Column option flags per casacore ColumnDesc.h:
        //   Direct = 1, Undefined = 2, FixedShape = 4
        let option = if is_array
            && matches!(
                col.column_type(),
                ColumnType::Array(ArrayShapeContract::Fixed { .. })
            ) {
            5 // ColumnDesc::Direct | ColumnDesc::FixedShape
        } else {
            0
        };

        ColumnDescContents {
            class_name,
            col_name: col.name().to_string(),
            comment: String::new(),
            // C++ casacore default: ColumnDesc stores "StandardStMan" regardless
            // of which DM is actually bound. The binding is in the ColumnSet.
            data_manager_type: "StandardStMan".to_string(),
            data_manager_group: "StandardStMan".to_string(),
            data_type: dt,
            option,
            nrdim,
            shape,
            max_length: 0,
            keywords: RecordValue::default(),
            is_array,
            primitive_type: pt,
        }
    }
}
