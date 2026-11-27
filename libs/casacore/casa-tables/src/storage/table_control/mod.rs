// SPDX-License-Identifier: LGPL-3.0-or-later
#![allow(dead_code)]

use std::mem::size_of;
use std::path::Path;

use casa_aipsio::{AipsIo, AipsOpenOption};
use casa_core::{
    ArrayValue, Complex32, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::StorageError;
use super::data_type::{
    CasacoreDataType, ColumnClassInfo, RECORD_COLUMN_CLASS_NAME, array_column_class_name,
    parse_column_class_name, scalar_column_class_name,
};
use crate::table_path::stored_table_dat_keywords;

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
    /// Maps RefTable row `i` → parent row `row_map[i]`.
    pub row_map: Vec<u64>,
}

impl RefTableDatContents {
    pub(crate) fn parent_column<'a>(&'a self, column: &'a str) -> &'a str {
        self.column_name_map
            .iter()
            .find(|(view, _)| view == column)
            .map_or(column, |(_, parent)| parent.as_str())
    }

    pub(crate) fn parent_row(&self, row: usize) -> Option<usize> {
        self.row_map
            .get(row)
            .and_then(|&parent| usize::try_from(parent).ok())
    }

    pub(crate) fn parent_rows(&self, rows: &[usize]) -> Result<Vec<usize>, StorageError> {
        rows.iter()
            .map(|&row| {
                self.parent_row(row).ok_or_else(|| {
                    StorageError::FormatMismatch(format!("reference row {row} is out of bounds"))
                })
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TableDatContents {
    pub nrrow: u64,
    pub big_endian: bool,
    pub table_desc: TableDescContents,
    pub column_set: ColumnSetContents,
    /// The name a renamed column has in the column set, by the column's
    /// current name: casacore's `originalName`, under which its storage
    /// manager knows it, which `Table::renameColumn` leaves as it was. The
    /// reader gives every `PlainColumnEntry` its column's current name (so
    /// that a column finds its manager by name) and keeps the name on file
    /// here for the writer; a column never renamed has no entry.
    pub names_on_file: std::collections::HashMap<String, String>,
    pub(super) asdm: super::asdm_stman::Managers,
}

impl TableDatContents {
    pub(super) fn asdm_column(
        &self,
        column: &ColumnDescContents,
    ) -> Result<super::asdm_stman::Column, StorageError> {
        let name = self
            .names_on_file
            .get(&column.col_name)
            .unwrap_or(&column.col_name);
        super::asdm_stman::Column::of(name, column.require_primitive_type()?)
    }

    /// Bring both of `table.dat`'s row counts up to `rows`, the table's rows
    /// as persisted, read from the disk (`table.lock`'s sync data) and not
    /// from a handle, before the file read from disk is written back, as
    /// casacore's `PlainTable::putFile` writes its `nrrow_p`.
    /// The file can trail the table: casacore publishes added rows in the
    /// sync data, which casa-rs takes as the table's count, and rewrites
    /// `table.dat` only at its next full write, which a process that never
    /// closes the table never makes (CASA 6.7.6's simulator after a mosaic
    /// predict, #880). casacore checks every storage manager's rows against
    /// the count the file gives. No count is lowered, as casa-rs opens a
    /// table with the largest count (#882).
    pub(crate) fn raise_row_count(&mut self, rows: u64) {
        let rows = rows.max(self.nrrow).max(self.column_set.nrrow);
        self.nrrow = rows;
        self.column_set.nrrow = rows;
    }
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

#[cfg(test)]
thread_local! {
    pub(crate) static CONTROL_FILE_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn read_table_header(io: &mut AipsIo) -> Result<(u32, u64, bool, String), StorageError> {
    let table_version = io.getstart("Table")?;

    let nrrow = if table_version >= 3 {
        io.get_u64()?
    } else {
        io.get_u32()? as u64
    };

    let format = io.get_u32()?;
    let big_endian = format == 0;

    let table_type = io.get_string()?;
    Ok((table_version, nrrow, big_endian, table_type))
}

/// Read just the common header to identify a saved reference without
/// parsing the full descriptor for every plain table.
pub(crate) fn is_reference_descriptor(path: &Path) -> Result<bool, StorageError> {
    let mut io = AipsIo::open(path, AipsOpenOption::Old)?;
    Ok(read_table_header(&mut io)?.3 == "RefTable")
}

mod decode_records;
mod encode_records;

pub(crate) use decode_records::read_iposition;
pub(crate) use decode_records::read_record;
pub(crate) use decode_records::read_ref_table_file;
pub(crate) use decode_records::read_table_dat;
pub(crate) use decode_records::read_table_dat_dispatch;
pub use decode_records::read_table_record_file;
pub(crate) use decode_records::write_record;
use decode_records::*;
pub(crate) use encode_records::deserialize_record_from_uchar;
pub(crate) use encode_records::serialize_record_to_uchar;
pub(crate) use encode_records::write_concat_table_dat;
pub(crate) use encode_records::write_iposition;
pub(crate) use encode_records::write_ref_table_stream;
use encode_records::*;

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

/// Writes `contents` to `path`, the `table.dat` inside a table's directory.
///
/// Absolute table keywords are stored relative to that table, as casacore
/// stores them ([`stored_table_dat_keywords`]).
pub(crate) fn write_table_dat(
    path: &Path,
    contents: &TableDatContents,
) -> Result<(), StorageError> {
    let table = path.parent().unwrap_or(path);
    let contents = stored_table_dat_keywords(contents, table);
    // Backpatch object lengths in memory, avoiding seek-triggered file flushes.
    std::fs::write(path, serialize_table_dat(&contents)?)?;
    Ok(())
}

/// Encode the same control file before a typed array's storage is admitted.
pub(crate) fn serialize_table_dat(contents: &TableDatContents) -> Result<Box<[u8]>, StorageError> {
    let mut io = AipsIo::new_write_only(std::io::Cursor::new(Vec::new()));
    write_table_dat_contents(&mut io, contents)?;
    let cursor: std::io::Cursor<Vec<u8>> = io.into_inner_typed()?;
    Ok(cursor.into_inner().into_boxed_slice())
}

fn write_table_dat_contents(
    io: &mut AipsIo,
    contents: &TableDatContents,
) -> Result<(), StorageError> {
    // Write Table version 2 (nrrow as u32)
    io.putstart("Table", 2)?;
    io.put_u32(contents.nrrow as u32)?;
    io.put_u32(if contents.big_endian { 0 } else { 1 })?;
    io.put_string("PlainTable")?;

    write_table_desc(io, &contents.table_desc)?;
    write_column_set(io, contents)?;

    io.putend()?;
    Ok(())
}

/// Write a RefTable table.dat file.
pub(crate) fn write_ref_table_dat(
    path: &Path,
    contents: &RefTableDatContents,
) -> Result<(), StorageError> {
    write_ref_table_stream(AipsIo::open(path, AipsOpenOption::New)?, contents)
}

/// `put` each of `array`'s elements in casacore's order.
fn put_each<T: Copy>(
    io: &mut AipsIo,
    array: &ArrayD<T>,
    put: fn(&mut AipsIo, T) -> casa_aipsio::AipsIoObjectResult<()>,
) -> Result<(), StorageError> {
    for &value in casacore_order(array) {
        put(io, value)?;
    }
    Ok(())
}

/// `array`'s elements in casacore's (Fortran) order, the first axis
/// fastest: its reversed axes in logical order, which is a contiguous walk
/// of memory for an array in Fortran layout.
fn casacore_order<T>(array: &ArrayD<T>) -> impl Iterator<Item = &T> {
    array.t().into_iter()
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

use crate::schema::{ColumnSchema, TableDescHeader, TableSchema};

impl TableDescContents {
    /// The description a save writes: the schema's header (name, version and
    /// comment), the table keywords and the column descriptions.
    fn from_schema_header(
        schema: &TableSchema,
        keywords: &RecordValue,
        columns: Vec<ColumnDescContents>,
    ) -> Self {
        let header = schema.header();
        Self {
            name: header.name.clone(),
            version: header.version.clone(),
            comment: header.comment.clone(),
            table_keywords: keywords.clone(),
            private_keywords: RecordValue::default(),
            columns,
        }
    }
}

impl TableDatContents {
    pub(crate) fn retained_heap_bytes(&self) -> Option<usize> {
        let asdm_bytes = self.asdm.retained_heap_bytes()?;
        let table_desc = &self.table_desc;
        let mut bytes = table_desc
            .name
            .capacity()
            .checked_add(asdm_bytes)?
            .checked_add(table_desc.version.capacity())?
            .checked_add(table_desc.comment.capacity())?
            .checked_add(table_desc.table_keywords.retained_heap_bytes()?)?
            .checked_add(table_desc.private_keywords.retained_heap_bytes()?)?
            .checked_add(
                table_desc
                    .columns
                    .capacity()
                    .checked_mul(size_of::<ColumnDescContents>())?,
            )?;
        for column in &table_desc.columns {
            bytes = bytes
                .checked_add(column.class_name.capacity())?
                .checked_add(column.col_name.capacity())?
                .checked_add(column.comment.capacity())?
                .checked_add(column.data_manager_type.capacity())?
                .checked_add(column.data_manager_group.capacity())?
                .checked_add(column.shape.capacity().checked_mul(size_of::<i32>())?)?
                .checked_add(column.keywords.retained_heap_bytes()?)?;
        }

        let column_set = &self.column_set;
        bytes = bytes
            .checked_add(
                column_set
                    .data_managers
                    .capacity()
                    .checked_mul(size_of::<DataManagerEntry>())?,
            )?
            .checked_add(
                column_set
                    .columns
                    .capacity()
                    .checked_mul(size_of::<PlainColumnEntry>())?,
            )?;
        for manager in &column_set.data_managers {
            bytes = bytes
                .checked_add(manager.type_name.capacity())?
                .checked_add(manager.data.capacity())?;
        }
        for column in &column_set.columns {
            bytes = bytes.checked_add(column.original_name.capacity())?;
        }
        // The names of renamed columns, as they stand and as the file has them.
        bytes = bytes.checked_add(
            self.names_on_file
                .capacity()
                .checked_mul(size_of::<(String, String)>())?,
        )?;
        for (current, on_file) in &self.names_on_file {
            bytes = bytes
                .checked_add(current.capacity())?
                .checked_add(on_file.capacity())?;
        }
        Some(bytes)
    }

    pub(crate) fn to_table_schema(&self) -> Result<TableSchema, StorageError> {
        let columns: Vec<ColumnSchema> = self
            .table_desc
            .columns
            .iter()
            .map(|c| c.to_column_schema())
            .collect::<Result<_, _>>()?;
        Ok(TableSchema::new(columns)?.with_header(TableDescHeader {
            name: self.table_desc.name.clone(),
            version: self.table_desc.version.clone(),
            comment: self.table_desc.comment.clone(),
        }))
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
            table_desc: TableDescContents::from_schema_header(schema, keywords, columns),
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
            names_on_file: std::collections::HashMap::new(),
            asdm: Default::default(),
        }
    }
    /// Build `TableDatContents` with both a storage manager DM and virtual engine DMs.
    ///
    /// Stored columns are bound to DM seq_nr 0 (the physical storage manager).
    /// Each virtual column binding gets a separate DM entry (seq_nr 1, 2, ...).
    /// Virtual engine keywords are injected into the column descriptors.
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::too_many_lines,
        reason = "predates the function-length limit; #705"
    )]
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
        use casa_core::{RecordField, ScalarValue, Value};

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
                        let rel_path = crate::table_path::strip_directory(ref_table, table_path);
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
                        let rel_path = crate::table_path::strip_directory(ref_table, table_path);
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
            table_desc: TableDescContents::from_schema_header(schema, keywords, columns),
            column_set: ColumnSetContents {
                nrrow,
                seq_count,
                data_managers: dm_entries,
                columns: plain_columns,
            },
            names_on_file: std::collections::HashMap::new(),
            asdm: Default::default(),
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
        Some(casa_core::PrimitiveType::Float32) => "float   ",
        Some(casa_core::PrimitiveType::Float64) => "double  ",
        Some(casa_core::PrimitiveType::Complex32) => "Complex ",
        Some(casa_core::PrimitiveType::Complex64) => "DComplex",
        _ => "double  ",
    };
    let stype = schema
        .columns()
        .iter()
        .find(|c| c.name() == stored_col)
        .and_then(|c| c.data_type());
    let stype_str = match stype {
        Some(casa_core::PrimitiveType::Int16) => "Short   ",
        Some(casa_core::PrimitiveType::Int32) => "Int     ",
        Some(casa_core::PrimitiveType::Int64) => "Int64   ",
        Some(casa_core::PrimitiveType::UInt8) => "uChar   ",
        Some(casa_core::PrimitiveType::UInt16) => "uShort  ",
        Some(casa_core::PrimitiveType::UInt32) => "uInt    ",
        Some(casa_core::PrimitiveType::Float32) => "float   ",
        Some(casa_core::PrimitiveType::Float64) => "double  ",
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
        Some(casa_core::PrimitiveType::Complex32) => "Complex ",
        Some(casa_core::PrimitiveType::Complex64) => "DComplex",
        _ => "Complex ",
    };
    let stype = schema
        .columns()
        .iter()
        .find(|c| c.name() == stored_col)
        .and_then(|c| c.data_type());
    let stype_str = match stype {
        Some(casa_core::PrimitiveType::Int16) => "Short   ",
        Some(casa_core::PrimitiveType::Int32) => "Int     ",
        Some(casa_core::PrimitiveType::Int64) => "Int64   ",
        Some(casa_core::PrimitiveType::UInt8) => "uChar   ",
        Some(casa_core::PrimitiveType::UInt16) => "uShort  ",
        Some(casa_core::PrimitiveType::UInt32) => "uInt    ",
        Some(casa_core::PrimitiveType::Float32) => "float   ",
        Some(casa_core::PrimitiveType::Float64) => "double  ",
        _ => "Short   ",
    };
    format!("ScaledComplexData<{vtype_str},{stype_str}>")
}

impl ColumnDescContents {
    fn to_column_schema(&self) -> Result<ColumnSchema, StorageError> {
        let options = crate::schema::ColumnOptions {
            direct: (self.option & 1) != 0,
            undefined: (self.option & 2) != 0,
        };

        // Record columns have data_type = TpRecord and no primitive type.
        if self.data_type == CasacoreDataType::TpRecord {
            return ColumnSchema::record(&self.col_name)
                .with_comment(self.comment.clone())
                .with_max_length(self.max_length)
                .with_options(options)
                .map_err(|err| StorageError::FormatMismatch(err.to_string()));
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
                ColumnSchema::array_fixed(&self.col_name, pt, shape)
                    .with_comment(self.comment.clone())
                    .with_max_length(self.max_length)
                    .with_options(options)
                    .map_err(|err| StorageError::FormatMismatch(err.to_string()))
            } else if self.nrdim > 0 {
                ColumnSchema::array_variable(&self.col_name, pt, Some(self.nrdim as usize))
                    .with_comment(self.comment.clone())
                    .with_max_length(self.max_length)
                    .with_options(options)
                    .map_err(|err| StorageError::FormatMismatch(err.to_string()))
            } else {
                ColumnSchema::array_variable(&self.col_name, pt, None)
                    .with_comment(self.comment.clone())
                    .with_max_length(self.max_length)
                    .with_options(options)
                    .map_err(|err| StorageError::FormatMismatch(err.to_string()))
            }
        } else {
            ColumnSchema::scalar(&self.col_name, pt)
                .with_comment(self.comment.clone())
                .with_max_length(self.max_length)
                .with_options(options)
                .map_err(|err| StorageError::FormatMismatch(err.to_string()))
        }
    }

    pub(crate) fn from_column_schema(col: &ColumnSchema) -> Self {
        use crate::schema::{ArrayShapeContract, ColumnType};

        // Record columns have no primitive type; use TpRecord directly.
        if matches!(col.column_type(), ColumnType::Record) {
            return ColumnDescContents {
                class_name: RECORD_COLUMN_CLASS_NAME.to_string(),
                col_name: col.name().to_string(),
                comment: col.comment().to_string(),
                data_manager_type: "StandardStMan".to_string(),
                data_manager_group: "StandardStMan".to_string(),
                data_type: CasacoreDataType::TpRecord,
                option: if col.options().undefined { 2 } else { 0 },
                nrdim: 0,
                shape: vec![],
                max_length: col.max_length(),
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
            // casacore's ArrayColumnDesc writes a dimensionality left open
            // as -1; readers take any value below 1 the same way.
            ColumnType::Array(ArrayShapeContract::Variable { ndim: None }) => (-1, vec![]),
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
        //
        // Fixed-shape arrays are not necessarily direct in casacore; preserve
        // the caller's Direct flag instead of forcing it on every fixed array.
        // Direct forces FixedShape, scalars included, as casacore's
        // BaseColumnDesc constructor does (a Direct scalar is option 5).
        let mut option = 0;
        if col.options().direct {
            option |= 1 | 4;
        }
        if col.options().undefined {
            option |= 2;
        }
        if is_array
            && matches!(
                col.column_type(),
                ColumnType::Array(ArrayShapeContract::Fixed { .. })
            )
        {
            option |= 4;
        }

        ColumnDescContents {
            class_name,
            col_name: col.name().to_string(),
            comment: col.comment().to_string(),
            data_manager_type: "StandardStMan".to_string(),
            data_manager_group: "StandardStMan".to_string(),
            data_type: dt,
            option,
            nrdim,
            shape,
            max_length: col.max_length(),
            keywords: RecordValue::default(),
            is_array,
            primitive_type: Some(pt),
        }
    }
}

#[cfg(test)]
mod tests;
