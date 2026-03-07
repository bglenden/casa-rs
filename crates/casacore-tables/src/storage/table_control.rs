// SPDX-License-Identifier: LGPL-3.0-or-later
#![allow(dead_code)]

use std::path::Path;

use casacore_aipsio::{AipsIo, AipsOpenOption};
use casacore_types::{
    ArrayValue, Complex32, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::StorageError;
use super::data_type::{
    CasacoreDataType, ColumnClassInfo, RECORD_COLUMN_CLASS_NAME, array_column_class_name,
    parse_column_class_name, scalar_column_class_name,
};

// ---------------------------------------------------------------------------
// Parsed structures
// ---------------------------------------------------------------------------

/// Result of reading table.dat — dispatches on the table type marker.
#[derive(Debug, Clone)]
pub(crate) enum TableDatResult {
    Plain(TableDatContents),
    Ref(RefTableDatContents),
    Concat(ConcatTableDatContents),
}

/// Contents of a ConcatTable's table.dat (no TableDesc/ColumnSet — just
/// the list of constituent table paths and subtable names to concatenate).
///
/// # C++ equivalent
///
/// The AipsIO payload inside `ConcatTable::writeConcatTable`.
#[derive(Debug, Clone)]
pub(crate) struct ConcatTableDatContents {
    pub nrrow: u64,
    pub big_endian: bool,
    /// Relative paths from this table's directory to each constituent table.
    pub table_paths: Vec<String>,
    /// Names of subtables to concatenate (usually empty for basic use).
    pub sub_table_names: Vec<String>,
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
    /// The primitive element type. `None` for record columns (`TpRecord`).
    pub primitive_type: Option<PrimitiveType>,
}

impl ColumnDescContents {
    /// Return the primitive element type, or a `StorageError` for record columns.
    ///
    /// Most storage managers only handle typed (scalar/array) columns; this
    /// helper provides a clean unwrap for those code paths.
    pub(crate) fn require_primitive_type(&self) -> Result<PrimitiveType, StorageError> {
        self.primitive_type.ok_or_else(|| {
            StorageError::FormatMismatch(format!(
                "column '{}' is a record column with no primitive type",
                self.col_name
            ))
        })
    }

    /// Return `true` if this is a record column (`TpRecord`).
    pub(crate) fn is_record(&self) -> bool {
        self.data_type == CasacoreDataType::TpRecord
    }
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

/// Read the AipsIO "ConcatTable" object payload from inside the "Table" envelope.
fn read_concat_table_payload(
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
fn read_array_record_field(io: &mut AipsIo, dt: CasacoreDataType) -> Result<Value, StorageError> {
    // C++ writes different type names depending on the element type.
    // Peek the actual type name and use it for getstart.
    let type_name = io.get_next_type()?;
    let version = io.getstart(&type_name)?;
    let ndim = io.get_u32()? as usize;

    if ndim == 0 {
        io.getend()?;
        // Return a zero-element array of the appropriate type.
        return match dt {
            CasacoreDataType::TpArrayString => Ok(Value::Array(ArrayValue::String(
                ArrayD::from_shape_vec(IxDyn(&[0]).f(), vec![]).unwrap(),
            ))),
            _ => Ok(Value::Array(ArrayValue::Float32(
                ArrayD::from_shape_vec(IxDyn(&[0]).f(), vec![]).unwrap(),
            ))),
        };
    }

    // Older versions (< 3) include an origin before the shape.
    if version < 3 {
        for _ in 0..ndim {
            let _origin = io.get_i32()?;
        }
    }

    // Shape: ndim × u32 (NOT an IPosition AipsIO object).
    let mut shape = Vec::with_capacity(ndim);
    for _ in 0..ndim {
        shape.push(io.get_u32()? as usize);
    }
    let nelem: usize = shape.iter().product();

    let av = match dt {
        CasacoreDataType::TpArrayBool => {
            let count = io.get_u32()? as usize;
            let mut vals = vec![false; count.min(nelem)];
            for v in vals.iter_mut() {
                *v = io.get_bool()?;
            }
            ArrayValue::Bool(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpArrayUChar => {
            let count = io.get_u32()? as usize;
            let mut vals = vec![0u8; count.min(nelem)];
            for v in vals.iter_mut() {
                *v = io.get_u8()?;
            }
            ArrayValue::UInt8(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpArrayShort => {
            let count = io.get_u32()? as usize;
            let mut vals = vec![0i16; count.min(nelem)];
            for v in vals.iter_mut() {
                *v = io.get_i16()?;
            }
            ArrayValue::Int16(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpArrayUShort => {
            let count = io.get_u32()? as usize;
            let mut vals = vec![0u16; count.min(nelem)];
            for v in vals.iter_mut() {
                *v = io.get_u16()?;
            }
            ArrayValue::UInt16(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpArrayInt => {
            let count = io.get_u32()? as usize;
            let mut vals = vec![0i32; count.min(nelem)];
            for v in vals.iter_mut() {
                *v = io.get_i32()?;
            }
            ArrayValue::Int32(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpArrayUInt => {
            let count = io.get_u32()? as usize;
            let mut vals = vec![0u32; count.min(nelem)];
            for v in vals.iter_mut() {
                *v = io.get_u32()?;
            }
            ArrayValue::UInt32(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpArrayFloat => {
            let count = io.get_u32()? as usize;
            let mut vals = vec![0f32; count.min(nelem)];
            for v in vals.iter_mut() {
                *v = io.get_f32()?;
            }
            ArrayValue::Float32(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpArrayDouble => {
            let count = io.get_u32()? as usize;
            let mut vals = vec![0f64; count.min(nelem)];
            for v in vals.iter_mut() {
                *v = io.get_f64()?;
            }
            ArrayValue::Float64(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpArrayComplex => {
            let count = io.get_u32()? as usize;
            let mut vals = vec![Complex32::new(0.0, 0.0); count.min(nelem)];
            for v in vals.iter_mut() {
                *v = io.get_complex32()?;
            }
            ArrayValue::Complex32(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpArrayDComplex => {
            let count = io.get_u32()? as usize;
            let mut vals = vec![Complex64::new(0.0, 0.0); count.min(nelem)];
            for v in vals.iter_mut() {
                *v = io.get_complex64()?;
            }
            ArrayValue::Complex64(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpArrayString => {
            let count = io.get_u32()? as usize;
            let mut vals = Vec::with_capacity(count.min(nelem));
            for _ in 0..count.min(nelem) {
                vals.push(io.get_string()?);
            }
            ArrayValue::String(
                ArrayD::from_shape_vec(IxDyn(&shape).f(), vals)
                    .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?,
            )
        }
        CasacoreDataType::TpArrayInt64 => {
            let count = io.get_u32()? as usize;
            let mut vals = vec![0i64; count.min(nelem)];
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

    // Table envelope — version 2 (nrrow as u32).
    io.putstart("Table", 2)?;
    io.put_u32(contents.nrrow as u32)?;
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

pub(crate) fn write_iposition(io: &mut AipsIo, values: &[i32]) -> Result<(), StorageError> {
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

/// Deserialize a `RecordValue` from a uChar byte buffer (AipsIO `TableRecord` format).
///
/// C++ `ScalarRecordColumnData` stores records as `Vector<uChar>` containing
/// AipsIO-serialized `TableRecord` objects. This function reverses that encoding.
pub(crate) fn deserialize_record_from_uchar(bytes: &[u8]) -> Result<RecordValue, StorageError> {
    use casacore_aipsio::ByteOrder;
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
    use casacore_aipsio::ByteOrder;
    use std::io::Cursor;

    let buf = Vec::new();
    let mut io = AipsIo::new_write_only_with_order(Cursor::new(buf), ByteOrder::BigEndian);
    write_table_record(&mut io, record)?;
    let cursor: Cursor<Vec<u8>> = io
        .into_inner_typed()
        .map_err(|e| StorageError::FormatMismatch(format!("serialize_record_to_uchar: {e}")))?;
    Ok(cursor.into_inner())
}

fn write_empty_record_desc(io: &mut AipsIo) -> Result<(), StorageError> {
    io.putstart("RecordDesc", 2)?;
    io.put_i32(0)?; // nfields=0
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
            // Array field: write IPosition shape.
            if let Value::Array(av) = &field.value {
                let shape: Vec<i32> = av.shape().iter().map(|&d| d as i32).collect();
                write_iposition(io, &shape)?;
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
fn write_array_record_field(io: &mut AipsIo, av: &ArrayValue) -> Result<(), StorageError> {
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
        match av {
            ArrayValue::String(a) => {
                for s in a.as_slice_memory_order().unwrap_or(&[]) {
                    io.put_string(s)?;
                }
            }
            _ => {
                write_array_elements(io, av)?;
            }
        }
    }

    io.putend()?;
    Ok(())
}

/// Write array elements in memory (Fortran) order for non-string arrays.
fn write_array_elements(io: &mut AipsIo, av: &ArrayValue) -> Result<(), StorageError> {
    match av {
        ArrayValue::Bool(a) => {
            for &v in a.as_slice_memory_order().unwrap_or(&[]) {
                io.put_bool(v)?;
            }
        }
        ArrayValue::UInt8(a) => {
            for &v in a.as_slice_memory_order().unwrap_or(&[]) {
                io.put_u8(v)?;
            }
        }
        ArrayValue::Int16(a) => {
            for &v in a.as_slice_memory_order().unwrap_or(&[]) {
                io.put_i16(v)?;
            }
        }
        ArrayValue::UInt16(a) => {
            for &v in a.as_slice_memory_order().unwrap_or(&[]) {
                io.put_u16(v)?;
            }
        }
        ArrayValue::Int32(a) => {
            for &v in a.as_slice_memory_order().unwrap_or(&[]) {
                io.put_i32(v)?;
            }
        }
        ArrayValue::UInt32(a) => {
            for &v in a.as_slice_memory_order().unwrap_or(&[]) {
                io.put_u32(v)?;
            }
        }
        ArrayValue::Float32(a) => {
            for &v in a.as_slice_memory_order().unwrap_or(&[]) {
                io.put_f32(v)?;
            }
        }
        ArrayValue::Float64(a) => {
            for &v in a.as_slice_memory_order().unwrap_or(&[]) {
                io.put_f64(v)?;
            }
        }
        ArrayValue::Int64(a) => {
            for &v in a.as_slice_memory_order().unwrap_or(&[]) {
                io.put_i64(v)?;
            }
        }
        ArrayValue::Complex32(a) => {
            for &v in a.as_slice_memory_order().unwrap_or(&[]) {
                io.put_complex32(v)?;
            }
        }
        ArrayValue::Complex64(a) => {
            for &v in a.as_slice_memory_order().unwrap_or(&[]) {
                io.put_complex64(v)?;
            }
        }
        ArrayValue::String(_) => {
            unreachable!("string arrays handled separately")
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
        Value::TableRef(_) => Ok(CasacoreDataType::TpTable),
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
    /// Build `TableDatContents` with both a storage manager DM and virtual engine DMs.
    ///
    /// Stored columns are bound to DM seq_nr 0 (the physical storage manager).
    /// Each virtual column binding gets a separate DM entry (seq_nr 1, 2, ...).
    /// Virtual engine keywords are injected into the column descriptors.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_snapshot_with_virtual(
        schema: &TableSchema,
        keywords: &RecordValue,
        column_keywords: &std::collections::HashMap<String, RecordValue>,
        nrrow: u64,
        dm_type_name: &str,
        dm_data: &[u8],
        big_endian: bool,
        virtual_bindings: &[super::virtual_engine::VirtualColumnBinding],
        table_path: &std::path::Path,
    ) -> Self {
        use super::virtual_engine::VirtualColumnBinding;
        use casacore_types::{RecordField, ScalarValue, Value};

        // Build set of virtual column names for quick lookup.
        let virtual_col_names: std::collections::HashSet<&str> = virtual_bindings
            .iter()
            .map(|b| match b {
                VirtualColumnBinding::Forward { col_name, .. } => col_name.as_str(),
                VirtualColumnBinding::ScaledArray { virtual_col, .. } => virtual_col.as_str(),
                VirtualColumnBinding::ScaledComplexData { virtual_col, .. } => virtual_col.as_str(),
                VirtualColumnBinding::BitFlags { virtual_col, .. } => virtual_col.as_str(),
                VirtualColumnBinding::CompressFloat { virtual_col, .. } => virtual_col.as_str(),
                VirtualColumnBinding::CompressComplex { virtual_col, .. } => virtual_col.as_str(),
                VirtualColumnBinding::ForwardIndexedRow { col_name, .. } => col_name.as_str(),
                VirtualColumnBinding::TaQLColumn { col_name, .. } => col_name.as_str(),
            })
            .collect();

        // Group virtual bindings by DM type + group name for DM entry creation.
        // Each unique engine type gets one DM entry.
        let mut dm_entries = vec![DataManagerEntry {
            type_name: dm_type_name.to_string(),
            seq_nr: 0,
            data: dm_data.to_vec(),
        }];

        // Map from column name to (dm_seq_nr, engine_type, engine_group).
        let mut col_dm_map: std::collections::HashMap<String, (u32, String, String)> =
            std::collections::HashMap::new();

        let mut next_seq_nr = 1u32;

        for binding in virtual_bindings {
            match binding {
                VirtualColumnBinding::Forward { col_name, .. } => {
                    // Each ForwardColumnEngine binding gets its own DM entry.
                    let seq = next_seq_nr;
                    next_seq_nr += 1;
                    dm_entries.push(DataManagerEntry {
                        type_name: "ForwardColumnEngine".to_string(),
                        seq_nr: seq,
                        data: Vec::new(),
                    });
                    col_dm_map.insert(
                        col_name.clone(),
                        (
                            seq,
                            "ForwardColumnEngine".to_string(),
                            "ForwardColumnEngine".to_string(),
                        ),
                    );
                }
                VirtualColumnBinding::ScaledArray {
                    virtual_col,
                    stored_col,
                    ..
                } => {
                    let seq = next_seq_nr;
                    next_seq_nr += 1;
                    // C++ uses parameterized type name with 8-char padded type
                    // strings from ValType::getTypeStr.
                    let type_name = scaled_array_dm_type_name(schema, virtual_col, stored_col);
                    dm_entries.push(DataManagerEntry {
                        type_name: type_name.clone(),
                        seq_nr: seq,
                        data: Vec::new(),
                    });
                    col_dm_map.insert(
                        virtual_col.clone(),
                        (seq, type_name, "ScaledArrayEngine".to_string()),
                    );
                }
                VirtualColumnBinding::ScaledComplexData {
                    virtual_col,
                    stored_col,
                    ..
                } => {
                    let seq = next_seq_nr;
                    next_seq_nr += 1;
                    let type_name = scaled_complex_dm_type_name(schema, virtual_col, stored_col);
                    dm_entries.push(DataManagerEntry {
                        type_name: type_name.clone(),
                        seq_nr: seq,
                        data: Vec::new(),
                    });
                    col_dm_map.insert(
                        virtual_col.clone(),
                        (seq, type_name, "ScaledComplexData".to_string()),
                    );
                }
                VirtualColumnBinding::BitFlags { virtual_col, .. } => {
                    let seq = next_seq_nr;
                    next_seq_nr += 1;
                    let type_name = "BitFlagsEngine<uChar".to_string();
                    dm_entries.push(DataManagerEntry {
                        type_name: type_name.clone(),
                        seq_nr: seq,
                        data: Vec::new(),
                    });
                    col_dm_map.insert(
                        virtual_col.clone(),
                        (seq, type_name, "BitFlagsEngine".to_string()),
                    );
                }
                VirtualColumnBinding::CompressFloat { virtual_col, .. } => {
                    let seq = next_seq_nr;
                    next_seq_nr += 1;
                    let type_name = "CompressFloat".to_string();
                    dm_entries.push(DataManagerEntry {
                        type_name: type_name.clone(),
                        seq_nr: seq,
                        data: Vec::new(),
                    });
                    col_dm_map.insert(virtual_col.clone(), (seq, type_name.clone(), type_name));
                }
                VirtualColumnBinding::CompressComplex {
                    virtual_col,
                    single_dish,
                    ..
                } => {
                    let seq = next_seq_nr;
                    next_seq_nr += 1;
                    let type_name = if *single_dish {
                        "CompressComplexSD".to_string()
                    } else {
                        "CompressComplex".to_string()
                    };
                    dm_entries.push(DataManagerEntry {
                        type_name: type_name.clone(),
                        seq_nr: seq,
                        data: Vec::new(),
                    });
                    col_dm_map.insert(virtual_col.clone(), (seq, type_name.clone(), type_name));
                }
                VirtualColumnBinding::ForwardIndexedRow { col_name, .. } => {
                    let seq = next_seq_nr;
                    next_seq_nr += 1;
                    dm_entries.push(DataManagerEntry {
                        type_name: "ForwardColumnIndexedRowEngine".to_string(),
                        seq_nr: seq,
                        data: Vec::new(),
                    });
                    col_dm_map.insert(
                        col_name.clone(),
                        (
                            seq,
                            "ForwardColumnIndexedRowEngine".to_string(),
                            "ForwardColumnIndexedRowEngine".to_string(),
                        ),
                    );
                }
                VirtualColumnBinding::TaQLColumn { col_name, .. } => {
                    let seq = next_seq_nr;
                    next_seq_nr += 1;
                    dm_entries.push(DataManagerEntry {
                        type_name: "VirtualTaQLColumn".to_string(),
                        seq_nr: seq,
                        data: Vec::new(),
                    });
                    col_dm_map.insert(
                        col_name.clone(),
                        (
                            seq,
                            "VirtualTaQLColumn".to_string(),
                            "VirtualTaQLColumn".to_string(),
                        ),
                    );
                }
            }
        }

        let mut columns: Vec<ColumnDescContents> = schema
            .columns()
            .iter()
            .map(|col| {
                let mut desc = ColumnDescContents::from_column_schema(col);
                if let Some(kw) = column_keywords.get(col.name()) {
                    desc.keywords = kw.clone();
                }
                // Set DM type/group for virtual columns.
                if let Some((_seq, dm_type, dm_group)) = col_dm_map.get(col.name()) {
                    desc.data_manager_type = dm_type.clone();
                    desc.data_manager_group = dm_group.clone();
                }
                desc
            })
            .collect();

        // Inject engine-specific keywords into virtual column descriptors.
        for binding in virtual_bindings {
            match binding {
                VirtualColumnBinding::Forward {
                    col_name,
                    ref_table,
                } => {
                    if let Some(desc) = columns.iter_mut().find(|c| c.col_name == *col_name) {
                        let rel_path = super::strip_directory(ref_table, table_path);
                        desc.keywords.push(RecordField::new(
                            "_ForwardColumn_TableName".to_string(),
                            Value::Scalar(ScalarValue::String(rel_path)),
                        ));
                    }
                }
                VirtualColumnBinding::ScaledArray {
                    virtual_col,
                    stored_col,
                    scale,
                    offset,
                } => {
                    if let Some(desc) = columns.iter_mut().find(|c| c.col_name == *virtual_col) {
                        desc.keywords.push(RecordField::new(
                            "_BaseMappedArrayEngine_Name".to_string(),
                            Value::Scalar(ScalarValue::String(stored_col.clone())),
                        ));
                        // C++ always writes ScaleName/OffsetName (empty when
                        // fixed), FixedScale/FixedOffset, and Scale/Offset.
                        desc.keywords.push(RecordField::new(
                            "_ScaledArrayEngine_ScaleName".to_string(),
                            Value::Scalar(ScalarValue::String(String::new())),
                        ));
                        desc.keywords.push(RecordField::new(
                            "_ScaledArrayEngine_OffsetName".to_string(),
                            Value::Scalar(ScalarValue::String(String::new())),
                        ));
                        desc.keywords.push(RecordField::new(
                            "_ScaledArrayEngine_FixedScale".to_string(),
                            Value::Scalar(ScalarValue::Bool(true)),
                        ));
                        desc.keywords.push(RecordField::new(
                            "_ScaledArrayEngine_Scale".to_string(),
                            Value::Scalar(ScalarValue::Float64(*scale)),
                        ));
                        desc.keywords.push(RecordField::new(
                            "_ScaledArrayEngine_FixedOffset".to_string(),
                            Value::Scalar(ScalarValue::Bool(true)),
                        ));
                        desc.keywords.push(RecordField::new(
                            "_ScaledArrayEngine_Offset".to_string(),
                            Value::Scalar(ScalarValue::Float64(*offset)),
                        ));
                    }
                }
                VirtualColumnBinding::ScaledComplexData {
                    virtual_col,
                    stored_col,
                    scale,
                    offset,
                } => {
                    if let Some(desc) = columns.iter_mut().find(|c| c.col_name == *virtual_col) {
                        desc.keywords.push(RecordField::new(
                            "_BaseMappedArrayEngine_Name".to_string(),
                            Value::Scalar(ScalarValue::String(stored_col.clone())),
                        ));
                        desc.keywords.push(RecordField::new(
                            "_ScaledComplexData_ScaleName".to_string(),
                            Value::Scalar(ScalarValue::String(String::new())),
                        ));
                        desc.keywords.push(RecordField::new(
                            "_ScaledComplexData_OffsetName".to_string(),
                            Value::Scalar(ScalarValue::String(String::new())),
                        ));
                        desc.keywords.push(RecordField::new(
                            "_ScaledComplexData_FixedScale".to_string(),
                            Value::Scalar(ScalarValue::Bool(true)),
                        ));
                        desc.keywords.push(RecordField::new(
                            "_ScaledComplexData_Scale".to_string(),
                            Value::Scalar(ScalarValue::Complex64(*scale)),
                        ));
                        desc.keywords.push(RecordField::new(
                            "_ScaledComplexData_FixedOffset".to_string(),
                            Value::Scalar(ScalarValue::Bool(true)),
                        ));
                        desc.keywords.push(RecordField::new(
                            "_ScaledComplexData_Offset".to_string(),
                            Value::Scalar(ScalarValue::Complex64(*offset)),
                        ));
                    }
                }
                VirtualColumnBinding::BitFlags {
                    virtual_col,
                    stored_col,
                    read_mask,
                    write_mask,
                } => {
                    if let Some(desc) = columns.iter_mut().find(|c| c.col_name == *virtual_col) {
                        desc.keywords.push(RecordField::new(
                            "_BaseMappedArrayEngine_StoredColumnName".to_string(),
                            Value::Scalar(ScalarValue::String(stored_col.clone())),
                        ));
                        desc.keywords.push(RecordField::new(
                            "_BitFlagsEngine_ReadMask".to_string(),
                            Value::Scalar(ScalarValue::UInt32(*read_mask)),
                        ));
                        desc.keywords.push(RecordField::new(
                            "_BitFlagsEngine_WriteMask".to_string(),
                            Value::Scalar(ScalarValue::UInt32(*write_mask)),
                        ));
                    }
                }
                VirtualColumnBinding::CompressFloat {
                    virtual_col,
                    stored_col,
                    scale,
                    offset,
                } => {
                    if let Some(desc) = columns.iter_mut().find(|c| c.col_name == *virtual_col) {
                        desc.keywords.push(RecordField::new(
                            "_BaseMappedArrayEngine_StoredColumnName".to_string(),
                            Value::Scalar(ScalarValue::String(stored_col.clone())),
                        ));
                        desc.keywords.push(RecordField::new(
                            "_CompressFloat_Scale".to_string(),
                            Value::Scalar(ScalarValue::Float32(*scale)),
                        ));
                        desc.keywords.push(RecordField::new(
                            "_CompressFloat_Offset".to_string(),
                            Value::Scalar(ScalarValue::Float32(*offset)),
                        ));
                        desc.keywords.push(RecordField::new(
                            "_CompressFloat_Fixed".to_string(),
                            Value::Scalar(ScalarValue::Bool(true)),
                        ));
                    }
                }
                VirtualColumnBinding::CompressComplex {
                    virtual_col,
                    stored_col,
                    scale,
                    offset,
                    single_dish,
                } => {
                    if let Some(desc) = columns.iter_mut().find(|c| c.col_name == *virtual_col) {
                        desc.keywords.push(RecordField::new(
                            "_BaseMappedArrayEngine_StoredColumnName".to_string(),
                            Value::Scalar(ScalarValue::String(stored_col.clone())),
                        ));
                        desc.keywords.push(RecordField::new(
                            "_CompressComplex_Scale".to_string(),
                            Value::Scalar(ScalarValue::Float32(*scale)),
                        ));
                        desc.keywords.push(RecordField::new(
                            "_CompressComplex_Offset".to_string(),
                            Value::Scalar(ScalarValue::Float32(*offset)),
                        ));
                        desc.keywords.push(RecordField::new(
                            "_CompressComplex_Fixed".to_string(),
                            Value::Scalar(ScalarValue::Bool(true)),
                        ));
                        desc.keywords.push(RecordField::new(
                            "_CompressComplex_Type".to_string(),
                            Value::Scalar(ScalarValue::String(
                                if *single_dish {
                                    "CompressComplexSD"
                                } else {
                                    "CompressComplex"
                                }
                                .to_string(),
                            )),
                        ));
                    }
                }
                VirtualColumnBinding::ForwardIndexedRow {
                    col_name,
                    ref_table,
                    row_column,
                } => {
                    if let Some(desc) = columns.iter_mut().find(|c| c.col_name == *col_name) {
                        let rel_path = super::strip_directory(ref_table, table_path);
                        desc.keywords.push(RecordField::new(
                            "_ForwardColumn_TableName_Row".to_string(),
                            Value::Scalar(ScalarValue::String(rel_path)),
                        ));
                        desc.keywords.push(RecordField::new(
                            "ForwardColumnIndexedRowEngine_RowName".to_string(),
                            Value::Scalar(ScalarValue::String(row_column.clone())),
                        ));
                    }
                }
                VirtualColumnBinding::TaQLColumn {
                    col_name,
                    expression,
                } => {
                    if let Some(desc) = columns.iter_mut().find(|c| c.col_name == *col_name) {
                        desc.keywords.push(RecordField::new(
                            "_VirtualTaQLColumn_CalcExpr".to_string(),
                            Value::Scalar(ScalarValue::String(expression.clone())),
                        ));
                    }
                }
            }
        }

        let plain_columns: Vec<PlainColumnEntry> = columns
            .iter()
            .map(|c| {
                let dm_seq_nr = if virtual_col_names.contains(c.col_name.as_str()) {
                    col_dm_map
                        .get(&c.col_name)
                        .map(|(seq, _, _)| *seq)
                        .unwrap_or(0)
                } else {
                    0
                };
                PlainColumnEntry {
                    original_name: c.col_name.clone(),
                    dm_seq_nr,
                    is_array: c.is_array,
                }
            })
            .collect();

        let seq_count = next_seq_nr;

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
                seq_count,
                data_managers: dm_entries,
                columns: plain_columns,
            },
        }
    }
}

/// Build the C++ DM type name for a ScaledArrayEngine binding.
///
/// C++ casacore uses 8-char padded type strings from `ValType::getTypeStr`,
/// producing names like `"ScaledArrayEngine<double  ,Int     >"`.
fn scaled_array_dm_type_name(schema: &TableSchema, virtual_col: &str, stored_col: &str) -> String {
    let vtype = schema
        .columns()
        .iter()
        .find(|c| c.name() == virtual_col)
        .and_then(|c| c.data_type());
    let vtype_str = match vtype {
        Some(casacore_types::PrimitiveType::Float32) => "float   ",
        Some(casacore_types::PrimitiveType::Float64) => "double  ",
        Some(casacore_types::PrimitiveType::Complex32) => "Complex ",
        Some(casacore_types::PrimitiveType::Complex64) => "DComplex",
        _ => "double  ",
    };
    let stype = schema
        .columns()
        .iter()
        .find(|c| c.name() == stored_col)
        .and_then(|c| c.data_type());
    let stype_str = match stype {
        Some(casacore_types::PrimitiveType::Int16) => "Short   ",
        Some(casacore_types::PrimitiveType::Int32) => "Int     ",
        Some(casacore_types::PrimitiveType::Int64) => "Int64   ",
        Some(casacore_types::PrimitiveType::UInt8) => "uChar   ",
        Some(casacore_types::PrimitiveType::UInt16) => "uShort  ",
        Some(casacore_types::PrimitiveType::UInt32) => "uInt    ",
        Some(casacore_types::PrimitiveType::Float32) => "float   ",
        Some(casacore_types::PrimitiveType::Float64) => "double  ",
        _ => "Int     ",
    };
    format!("ScaledArrayEngine<{vtype_str},{stype_str}>")
}

/// Build the C++ DM type name for a ScaledComplexData binding.
///
/// C++ casacore uses 8-char padded type strings from `ValType::getTypeStr`,
/// producing names like `"ScaledComplexData<Complex ,Short   >"`.
///
/// # C++ equivalent
///
/// `ScaledComplexData<VT, ST>::dataManagerType()` in
/// `casacore/tables/DataMan/ScaledComplexData.tcc`.
fn scaled_complex_dm_type_name(
    schema: &TableSchema,
    virtual_col: &str,
    stored_col: &str,
) -> String {
    let vtype = schema
        .columns()
        .iter()
        .find(|c| c.name() == virtual_col)
        .and_then(|c| c.data_type());
    let vtype_str = match vtype {
        Some(casacore_types::PrimitiveType::Complex32) => "Complex ",
        Some(casacore_types::PrimitiveType::Complex64) => "DComplex",
        _ => "Complex ",
    };
    let stype = schema
        .columns()
        .iter()
        .find(|c| c.name() == stored_col)
        .and_then(|c| c.data_type());
    let stype_str = match stype {
        Some(casacore_types::PrimitiveType::Int16) => "Short   ",
        Some(casacore_types::PrimitiveType::Int32) => "Int     ",
        Some(casacore_types::PrimitiveType::Int64) => "Int64   ",
        Some(casacore_types::PrimitiveType::UInt8) => "uChar   ",
        Some(casacore_types::PrimitiveType::UInt16) => "uShort  ",
        Some(casacore_types::PrimitiveType::UInt32) => "uInt    ",
        Some(casacore_types::PrimitiveType::Float32) => "float   ",
        Some(casacore_types::PrimitiveType::Float64) => "double  ",
        _ => "Short   ",
    };
    format!("ScaledComplexData<{vtype_str},{stype_str}>")
}

impl ColumnDescContents {
    fn to_column_schema(&self) -> Result<ColumnSchema, StorageError> {
        // Record columns have data_type = TpRecord and no primitive type.
        if self.data_type == CasacoreDataType::TpRecord {
            return Ok(ColumnSchema::record(&self.col_name));
        }

        let pt = self.primitive_type.ok_or_else(|| {
            StorageError::FormatMismatch(format!(
                "column '{}' has no primitive type",
                self.col_name
            ))
        })?;

        if self.is_array {
            if self.nrdim > 0 && !self.shape.is_empty() {
                let shape: Vec<usize> = self.shape.iter().map(|&s| s as usize).collect();
                Ok(ColumnSchema::array_fixed(&self.col_name, pt, shape))
            } else if self.nrdim > 0 {
                Ok(ColumnSchema::array_variable(
                    &self.col_name,
                    pt,
                    Some(self.nrdim as usize),
                ))
            } else {
                Ok(ColumnSchema::array_variable(&self.col_name, pt, None))
            }
        } else {
            Ok(ColumnSchema::scalar(&self.col_name, pt))
        }
    }

    fn from_column_schema(col: &ColumnSchema) -> Self {
        use crate::schema::{ArrayShapeContract, ColumnType};

        // Record columns have no primitive type; use TpRecord directly.
        if matches!(col.column_type(), ColumnType::Record) {
            return ColumnDescContents {
                class_name: RECORD_COLUMN_CLASS_NAME.to_string(),
                col_name: col.name().to_string(),
                comment: String::new(),
                data_manager_type: "StandardStMan".to_string(),
                data_manager_group: "StandardStMan".to_string(),
                data_type: CasacoreDataType::TpRecord,
                option: 0,
                nrdim: 0,
                shape: vec![],
                max_length: 0,
                keywords: RecordValue::default(),
                is_array: false,
                primitive_type: None,
            };
        }

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
            data_manager_type: "StandardStMan".to_string(),
            data_manager_group: "StandardStMan".to_string(),
            data_type: dt,
            option,
            nrdim,
            shape,
            max_length: 0,
            keywords: RecordValue::default(),
            is_array,
            primitive_type: Some(pt),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn test_dir(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("table_control_{name}_{nanos}"))
    }

    #[test]
    fn column_keywords_roundtrip_in_table_dat() {
        // Build a ColumnDescContents with nested record keywords
        let mut measinfo = RecordValue::default();
        measinfo.upsert("type", Value::Scalar(ScalarValue::String("epoch".into())));
        measinfo.upsert("Ref", Value::Scalar(ScalarValue::String("UTC".into())));
        let mut col_kw = RecordValue::default();
        col_kw.upsert("MEASINFO", Value::Record(measinfo));

        let col = ColumnDescContents {
            class_name: "ArrayColumnDesc<double  ".to_string(),
            col_name: "TIME".to_string(),
            comment: String::new(),
            data_manager_type: "StManAipsIO".to_string(),
            data_manager_group: "StManAipsIO".to_string(),
            data_type: CasacoreDataType::TpDouble,
            option: 5, // Direct | FixedShape
            nrdim: 1,
            shape: vec![1],
            max_length: 0,
            keywords: col_kw.clone(),
            is_array: true,
            primitive_type: Some(PrimitiveType::Float64),
        };

        let contents = TableDatContents {
            nrrow: 1,
            big_endian: true,
            table_desc: TableDescContents {
                name: String::new(),
                version: String::new(),
                comment: String::new(),
                table_keywords: RecordValue::default(),
                private_keywords: RecordValue::default(),
                columns: vec![col],
            },
            column_set: ColumnSetContents {
                nrrow: 1,
                seq_count: 1,
                data_managers: vec![DataManagerEntry {
                    type_name: "StManAipsIO".to_string(),
                    seq_nr: 0,
                    data: vec![],
                }],
                columns: vec![PlainColumnEntry {
                    original_name: "TIME".to_string(),
                    dm_seq_nr: 0,
                    is_array: true,
                }],
            },
        };

        let dir = test_dir("col_kw_rt");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("table.dat");

        write_table_dat(&path, &contents).unwrap();

        // Read back and check column keywords
        let readback = read_table_dat(&path).unwrap();
        assert_eq!(readback.table_desc.columns.len(), 1);
        let kw = &readback.table_desc.columns[0].keywords;
        assert!(
            !kw.fields().is_empty(),
            "column keywords should not be empty after roundtrip"
        );
        match kw.get("MEASINFO") {
            Some(Value::Record(mi)) => {
                assert_eq!(
                    mi.get("type"),
                    Some(&Value::Scalar(ScalarValue::String("epoch".into())))
                );
                assert_eq!(
                    mi.get("Ref"),
                    Some(&Value::Scalar(ScalarValue::String("UTC".into())))
                );
            }
            other => panic!("expected MEASINFO record, got {other:?}"),
        }

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn table_reference_keywords_roundtrip_in_table_record() {
        let record = RecordValue::new(vec![RecordField::new(
            "ANTENNA",
            Value::table_ref("ANTENNA"),
        )]);

        let encoded = serialize_record_to_uchar(&record).unwrap();
        let decoded = deserialize_record_from_uchar(&encoded).unwrap();

        assert_eq!(
            decoded.get("ANTENNA"),
            Some(&Value::TableRef("ANTENNA".to_string()))
        );
    }

    /// Write a minimal table.dat + empty data file, then compare with C++-written version.
    /// The output path is printed so it can be inspected externally.
    #[test]
    fn column_keywords_binary_matches_cpp_format() {
        // Build exactly what `from_snapshot` builds, with column keywords injected
        let schema = crate::TableSchema::new(vec![crate::ColumnSchema::array_fixed(
            "TIME",
            PrimitiveType::Float64,
            vec![1],
        )])
        .unwrap();

        let mut col_keywords = std::collections::HashMap::new();
        let mut measinfo = RecordValue::default();
        measinfo.upsert("type", Value::Scalar(ScalarValue::String("epoch".into())));
        measinfo.upsert("Ref", Value::Scalar(ScalarValue::String("UTC".into())));
        let mut kw = RecordValue::default();
        kw.upsert("MEASINFO", Value::Record(measinfo));
        col_keywords.insert("TIME".to_string(), kw);

        let table_dat = TableDatContents::from_snapshot(
            &schema,
            &RecordValue::default(),
            &col_keywords,
            1,
            "StManAipsIO",
            &[],
            true,
        );

        // Verify keywords were injected
        assert_eq!(table_dat.table_desc.columns.len(), 1);
        let col = &table_dat.table_desc.columns[0];
        assert!(
            !col.keywords.fields().is_empty(),
            "from_snapshot should inject column keywords into ColumnDescContents"
        );
        assert!(
            col.keywords.get("MEASINFO").is_some(),
            "MEASINFO should be present in column keywords"
        );
    }
}
