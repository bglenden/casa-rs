#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};

use casacore_aipsio::{AipsIo, AipsOpenOption};
use casacore_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use thiserror::Error;

use crate::schema::{
    ArrayShapeContract, ColumnOptions, ColumnSchema, ColumnType, SchemaError, TableSchema,
};

pub(crate) const TABLE_CONTROL_FILE: &str = "table.dat";
pub(crate) const TABLE_ROW_FILE: &str = "table.f0";

const CONTROL_OBJECT_TYPE: &str = "Table";
const CONTROL_OBJECT_VERSION: u32 = 1;
const CONTROL_TABLE_KIND: &str = "PlainTable";
const CONTROL_MANAGER_KIND: &str = "StManAipsIO";
const ROWS_OBJECT_TYPE: &str = "CasaRsRows";
const ROWS_OBJECT_VERSION: u32 = 2;

const SCHEMA_FIELD_PRESENT: &str = "present";
const SCHEMA_FIELD_VERSION: &str = "schema_version";
const SCHEMA_FIELD_COLUMN_COUNT: &str = "column_count";
const SCHEMA_VERSION: u32 = 1;

const COLUMN_FIELD_NAME: &str = "name";
const COLUMN_FIELD_KIND: &str = "kind";
const COLUMN_FIELD_DIRECT: &str = "direct";
const COLUMN_FIELD_UNDEFINED: &str = "undefined";
const COLUMN_FIELD_ARRAY_CONTRACT: &str = "array_contract";
const COLUMN_FIELD_SHAPE: &str = "shape";
const COLUMN_FIELD_NDIM: &str = "ndim";

#[derive(Debug, Error)]
pub(crate) enum StorageError {
    #[error("table path does not exist: {0}")]
    MissingPath(PathBuf),
    #[error("table control file is missing: {0}")]
    MissingControlFile(PathBuf),
    #[error("table row file is missing: {0}")]
    MissingRowFile(PathBuf),
    #[error("table control file mismatch: {0}")]
    ControlMismatch(&'static str),
    #[error("table rows file mismatch: {0}")]
    RowDataMismatch(&'static str),
    #[error("table schema mismatch: {0}")]
    SchemaMismatch(String),
    #[error("schema uses unsupported column count {0}")]
    ColumnCountTooLarge(usize),
    #[error("schema shape axis {0} is too large to encode")]
    ShapeAxisTooLarge(usize),
    #[error("schema shape value {0} does not fit target size")]
    ShapeValueTooLarge(u32),
    #[error("unsupported row count {0}")]
    RowCountTooLarge(u64),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("aipsio error: {0}")]
    AipsIo(#[from] casacore_aipsio::AipsIoObjectError),
}

impl From<SchemaError> for StorageError {
    fn from(value: SchemaError) -> Self {
        Self::SchemaMismatch(value.to_string())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StorageSnapshot {
    pub(crate) rows: Vec<RecordValue>,
    pub(crate) keywords: RecordValue,
    pub(crate) schema: Option<TableSchema>,
}

pub(crate) trait StorageManager {
    fn load(&self, table_path: &Path) -> Result<StorageSnapshot, StorageError>;
    fn save(&self, table_path: &Path, snapshot: &StorageSnapshot) -> Result<(), StorageError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct StManAipsIoStorage;

impl StorageManager for StManAipsIoStorage {
    fn load(&self, table_path: &Path) -> Result<StorageSnapshot, StorageError> {
        if !table_path.exists() {
            return Err(StorageError::MissingPath(table_path.to_path_buf()));
        }

        let control_path = table_path.join(TABLE_CONTROL_FILE);
        if !control_path.is_file() {
            return Err(StorageError::MissingControlFile(control_path));
        }
        let row_path = table_path.join(TABLE_ROW_FILE);
        if !row_path.is_file() {
            return Err(StorageError::MissingRowFile(row_path));
        }

        let mut control = AipsIo::open(&control_path, AipsOpenOption::Old)?;
        control.getstart(CONTROL_OBJECT_TYPE)?;
        let table_kind = control.get_string()?;
        if table_kind != CONTROL_TABLE_KIND {
            return Err(StorageError::ControlMismatch(
                "expected PlainTable control kind",
            ));
        }
        let manager_kind = control.get_string()?;
        if manager_kind != CONTROL_MANAGER_KIND {
            return Err(StorageError::ControlMismatch(
                "expected StManAipsIO storage manager marker",
            ));
        }
        control.getend()?;
        control.close()?;

        let mut row_io = AipsIo::open(&row_path, AipsOpenOption::Old)?;
        let row_version = row_io.getstart(ROWS_OBJECT_TYPE)?;

        let schema = match row_version {
            1 => None,
            2 => {
                let schema_value = row_io.get_value()?;
                decode_schema_value(schema_value)?
            }
            _ => {
                return Err(StorageError::RowDataMismatch(
                    "unsupported table row payload version",
                ));
            }
        };

        let keyword_value = row_io.get_value()?;
        let keywords = match keyword_value {
            Value::Record(record) => record,
            _ => {
                return Err(StorageError::RowDataMismatch(
                    "table keywords payload was not a record",
                ));
            }
        };

        let row_count_u64 = row_io.get_u64()?;
        let row_count = usize::try_from(row_count_u64)
            .map_err(|_| StorageError::RowCountTooLarge(row_count_u64))?;
        let mut rows = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let value = row_io.get_value()?;
            match value {
                Value::Record(record) => rows.push(record),
                _ => {
                    return Err(StorageError::RowDataMismatch(
                        "table row payload contained non-record value",
                    ));
                }
            }
        }
        row_io.getend()?;
        row_io.close()?;

        Ok(StorageSnapshot {
            rows,
            keywords,
            schema,
        })
    }

    fn save(&self, table_path: &Path, snapshot: &StorageSnapshot) -> Result<(), StorageError> {
        fs::create_dir_all(table_path)?;

        let control_path = table_path.join(TABLE_CONTROL_FILE);
        let mut control = AipsIo::open(&control_path, AipsOpenOption::New)?;
        control.putstart(CONTROL_OBJECT_TYPE, CONTROL_OBJECT_VERSION)?;
        control.put_string(CONTROL_TABLE_KIND)?;
        control.put_string(CONTROL_MANAGER_KIND)?;
        control.putend()?;
        control.close()?;

        let row_path = table_path.join(TABLE_ROW_FILE);
        let mut row_io = AipsIo::open(&row_path, AipsOpenOption::New)?;
        row_io.putstart(ROWS_OBJECT_TYPE, ROWS_OBJECT_VERSION)?;
        row_io.put_value(&encode_schema_value(snapshot.schema.as_ref())?)?;
        row_io.put_value(&Value::Record(snapshot.keywords.clone()))?;
        let row_count = u64::try_from(snapshot.rows.len())
            .map_err(|_| StorageError::RowCountTooLarge(u64::MAX))?;
        row_io.put_u64(row_count)?;
        for row in &snapshot.rows {
            row_io.put_value(&Value::Record(row.clone()))?;
        }
        row_io.putend()?;
        row_io.close()?;
        Ok(())
    }
}

fn encode_schema_value(schema: Option<&TableSchema>) -> Result<Value, StorageError> {
    if schema.is_none() {
        return Ok(Value::Record(RecordValue::new(vec![RecordField::new(
            SCHEMA_FIELD_PRESENT,
            Value::Scalar(ScalarValue::Bool(false)),
        )])));
    }

    let schema = schema.expect("checked is_some");
    let column_count = u32::try_from(schema.columns().len())
        .map_err(|_| StorageError::ColumnCountTooLarge(schema.columns().len()))?;

    let mut fields = Vec::with_capacity(schema.columns().len() + 3);
    fields.push(RecordField::new(
        SCHEMA_FIELD_PRESENT,
        Value::Scalar(ScalarValue::Bool(true)),
    ));
    fields.push(RecordField::new(
        SCHEMA_FIELD_VERSION,
        Value::Scalar(ScalarValue::UInt32(SCHEMA_VERSION)),
    ));
    fields.push(RecordField::new(
        SCHEMA_FIELD_COLUMN_COUNT,
        Value::Scalar(ScalarValue::UInt32(column_count)),
    ));

    for (index, column) in schema.columns().iter().enumerate() {
        fields.push(RecordField::new(
            format!("column_{index}"),
            Value::Record(encode_column_schema(column)?),
        ));
    }

    Ok(Value::Record(RecordValue::new(fields)))
}

fn encode_column_schema(column: &ColumnSchema) -> Result<RecordValue, StorageError> {
    let mut fields = vec![
        RecordField::new(
            COLUMN_FIELD_NAME,
            Value::Scalar(ScalarValue::String(column.name().to_string())),
        ),
        RecordField::new(
            COLUMN_FIELD_DIRECT,
            Value::Scalar(ScalarValue::Bool(column.options().direct)),
        ),
        RecordField::new(
            COLUMN_FIELD_UNDEFINED,
            Value::Scalar(ScalarValue::Bool(column.options().undefined)),
        ),
    ];

    match column.column_type() {
        ColumnType::Scalar => {
            fields.push(RecordField::new(
                COLUMN_FIELD_KIND,
                Value::Scalar(ScalarValue::String("scalar".to_string())),
            ));
        }
        ColumnType::Record => {
            fields.push(RecordField::new(
                COLUMN_FIELD_KIND,
                Value::Scalar(ScalarValue::String("record".to_string())),
            ));
        }
        ColumnType::Array(contract) => {
            fields.push(RecordField::new(
                COLUMN_FIELD_KIND,
                Value::Scalar(ScalarValue::String("array".to_string())),
            ));
            match contract {
                ArrayShapeContract::Fixed { shape } => {
                    let mut shape_u32 = Vec::with_capacity(shape.len());
                    for axis in shape {
                        shape_u32.push(
                            u32::try_from(*axis)
                                .map_err(|_| StorageError::ShapeAxisTooLarge(*axis))?,
                        );
                    }
                    fields.push(RecordField::new(
                        COLUMN_FIELD_ARRAY_CONTRACT,
                        Value::Scalar(ScalarValue::String("fixed".to_string())),
                    ));
                    fields.push(RecordField::new(
                        COLUMN_FIELD_SHAPE,
                        Value::Array(ArrayValue::from_u32_vec(shape_u32)),
                    ));
                }
                ArrayShapeContract::Variable { ndim } => {
                    let ndim_value = match ndim {
                        Some(ndim) => i32::try_from(*ndim).map_err(|_| {
                            StorageError::SchemaMismatch(format!(
                                "column \"{}\" ndim too large",
                                column.name()
                            ))
                        })?,
                        None => -1,
                    };
                    fields.push(RecordField::new(
                        COLUMN_FIELD_ARRAY_CONTRACT,
                        Value::Scalar(ScalarValue::String("variable".to_string())),
                    ));
                    fields.push(RecordField::new(
                        COLUMN_FIELD_NDIM,
                        Value::Scalar(ScalarValue::Int32(ndim_value)),
                    ));
                }
            }
        }
    }

    Ok(RecordValue::new(fields))
}

fn decode_schema_value(value: Value) -> Result<Option<TableSchema>, StorageError> {
    let record = match value {
        Value::Record(record) => record,
        _ => {
            return Err(StorageError::SchemaMismatch(
                "schema payload was not a record".to_string(),
            ));
        }
    };

    let present = expect_bool(&record, SCHEMA_FIELD_PRESENT)?;
    if !present {
        return Ok(None);
    }

    let schema_version = expect_u32(&record, SCHEMA_FIELD_VERSION)?;
    if schema_version != SCHEMA_VERSION {
        return Err(StorageError::SchemaMismatch(format!(
            "unsupported schema version {schema_version}"
        )));
    }

    let column_count = expect_u32(&record, SCHEMA_FIELD_COLUMN_COUNT)? as usize;
    let mut columns = Vec::with_capacity(column_count);
    for index in 0..column_count {
        let field_name = format!("column_{index}");
        let column_record = match record.get(&field_name) {
            Some(Value::Record(record)) => record,
            _ => {
                return Err(StorageError::SchemaMismatch(format!(
                    "missing schema column entry \"{field_name}\""
                )));
            }
        };
        columns.push(decode_column_schema(column_record)?);
    }
    Ok(Some(TableSchema::new(columns)?))
}

fn decode_column_schema(record: &RecordValue) -> Result<ColumnSchema, StorageError> {
    let name = expect_string(record, COLUMN_FIELD_NAME)?.to_string();
    let direct = expect_bool(record, COLUMN_FIELD_DIRECT)?;
    let undefined = expect_bool(record, COLUMN_FIELD_UNDEFINED)?;
    let options = ColumnOptions { direct, undefined };

    let kind = expect_string(record, COLUMN_FIELD_KIND)?;
    let column = match kind {
        "scalar" => ColumnSchema::scalar(name),
        "record" => ColumnSchema::record(name),
        "array" => {
            let contract = expect_string(record, COLUMN_FIELD_ARRAY_CONTRACT)?;
            match contract {
                "fixed" => {
                    let shape = expect_u32_array(record, COLUMN_FIELD_SHAPE)?;
                    ColumnSchema::array_fixed(name, shape)
                }
                "variable" => {
                    let ndim_i32 = expect_i32(record, COLUMN_FIELD_NDIM)?;
                    let ndim = if ndim_i32 < 0 {
                        None
                    } else {
                        Some(ndim_i32 as usize)
                    };
                    ColumnSchema::array_variable(name, ndim)
                }
                other => {
                    return Err(StorageError::SchemaMismatch(format!(
                        "unsupported array contract \"{other}\""
                    )));
                }
            }
        }
        other => {
            return Err(StorageError::SchemaMismatch(format!(
                "unsupported column kind \"{other}\""
            )));
        }
    };

    Ok(column.with_options(options)?)
}

fn expect_bool(record: &RecordValue, field: &str) -> Result<bool, StorageError> {
    match record.get(field) {
        Some(Value::Scalar(ScalarValue::Bool(value))) => Ok(*value),
        _ => Err(StorageError::SchemaMismatch(format!(
            "field \"{field}\" was not a bool scalar"
        ))),
    }
}

fn expect_u32(record: &RecordValue, field: &str) -> Result<u32, StorageError> {
    match record.get(field) {
        Some(Value::Scalar(ScalarValue::UInt32(value))) => Ok(*value),
        _ => Err(StorageError::SchemaMismatch(format!(
            "field \"{field}\" was not a u32 scalar"
        ))),
    }
}

fn expect_i32(record: &RecordValue, field: &str) -> Result<i32, StorageError> {
    match record.get(field) {
        Some(Value::Scalar(ScalarValue::Int32(value))) => Ok(*value),
        _ => Err(StorageError::SchemaMismatch(format!(
            "field \"{field}\" was not an i32 scalar"
        ))),
    }
}

fn expect_string<'a>(record: &'a RecordValue, field: &str) -> Result<&'a str, StorageError> {
    match record.get(field) {
        Some(Value::Scalar(ScalarValue::String(value))) => Ok(value.as_str()),
        _ => Err(StorageError::SchemaMismatch(format!(
            "field \"{field}\" was not a string scalar"
        ))),
    }
}

fn expect_u32_array(record: &RecordValue, field: &str) -> Result<Vec<usize>, StorageError> {
    let array = match record.get(field) {
        Some(Value::Array(ArrayValue::UInt32(values))) => values,
        _ => {
            return Err(StorageError::SchemaMismatch(format!(
                "field \"{field}\" was not a u32 array"
            )));
        }
    };

    let mut shape = Vec::with_capacity(array.len());
    for axis in array {
        shape.push(usize::try_from(*axis).map_err(|_| StorageError::ShapeValueTooLarge(*axis))?);
    }
    Ok(shape)
}
