// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `table` tool's read side: what the open table says about itself and
//! its columns, cells and keywords, as CASA 6.7.6.14's `tb` answers
//! (`tools/casa-compat/table_tool_oracle.py`). With no table open each call
//! logs and returns the empty answer CASA gives.
//!
//! One departure, recorded in `docs/CASA (C++) bugs.md`: CASA 6.7.6.14's
//! `getcol` of a column of `int64` raises "Unknown casa DataType!"; casa-rs
//! returns the `int64` array.

use casa_tables::{ColumnSchema, ColumnType};
use casa_types::{PrimitiveType, RecordValue, ScalarValue, Value};

use super::state::{Fail, Open, State, no_table_specified};
use super::values;
use crate::convert::value::CasaValue;
use crate::tools::LogLine;

type Reply = Result<CasaValue, Fail>;

/// A keyword named by `getkeyword`: by name (a dotted name reaches into
/// sub-records) or by position; `Whole` is no argument or `[]`.
#[derive(Debug, Clone)]
pub(crate) enum KeywordRef {
    Whole,
    Name(String),
    Index(i64),
}

/// What casacore says of a keyword position no record field has.
const NO_SUCH_FIELD: &str = "RecordInterface::asValueHolder - unknown data type";

/// What casatools returns for a call it could not serve: an empty `bool`
/// array.
fn empty_array() -> CasaValue {
    CasaValue::Bools {
        shape: vec![0],
        values: Vec::new(),
    }
}

fn texts(names: impl IntoIterator<Item = String>) -> CasaValue {
    CasaValue::List(names.into_iter().map(CasaValue::Text).collect())
}

fn runtime<T>(message: String) -> Result<T, Fail> {
    Err(Fail::Runtime(message))
}

/// casacore's name for a column's data type.
fn data_type_name(column: &ColumnSchema) -> &'static str {
    match column.data_type() {
        None => "record",
        Some(PrimitiveType::Bool) => "boolean",
        Some(PrimitiveType::UInt8) => "uchar",
        Some(PrimitiveType::UInt16) => "ushort",
        Some(PrimitiveType::UInt32) => "uint",
        Some(PrimitiveType::Int16) => "short",
        Some(PrimitiveType::Int32) => "int",
        Some(PrimitiveType::Int64) => "int64",
        Some(PrimitiveType::Float32) => "float",
        Some(PrimitiveType::Float64) => "double",
        Some(PrimitiveType::Complex32) => "complex",
        Some(PrimitiveType::Complex64) => "dcomplex",
        Some(PrimitiveType::String) => "string",
    }
}

/// The rows `start`, `start + incr`, ... for `count` rows (`-1`, or more
/// than there are: to the end), or `refusal` when `start` is beyond the row
/// after the last (a start at that row selects no rows). A step below one is
/// refused as casacore's `getColumn` refuses it.
pub(super) fn rows(
    nrows: usize,
    start: i64,
    count: i64,
    incr: i64,
    refusal: &str,
) -> Result<Vec<usize>, Fail> {
    rows_stepping(nrows, start, count, incr, refusal, "getColumn: rowincr<=0")
}

/// As [`rows`], with the refusal of a step below one that a method words in
/// its own way (`TableProxy::getColumnShapeString: rowincr<=0`).
pub(super) fn rows_stepping(
    nrows: usize,
    start: i64,
    count: i64,
    incr: i64,
    refusal: &str,
    step_refusal: &str,
) -> Result<Vec<usize>, Fail> {
    // casacore's `TableProxy::checkRowColumn`: the start is a row or the row
    // after the last when no rows are asked for (a count of `-1` asks for
    // all that follow, so for none here), and only then is the step checked.
    let Ok(start) = usize::try_from(start) else {
        return runtime(refusal.into());
    };
    if start > nrows || (start == nrows && count > 0) {
        return runtime(refusal.into());
    }
    if incr < 1 {
        return runtime(step_refusal.into());
    }
    let incr = usize::try_from(incr).unwrap_or(usize::MAX);
    let available = nrows.saturating_sub(start).div_ceil(incr);
    let count = usize::try_from(count).map_or(available, |count| count.min(available));
    Ok((0..count).map(|index| start + index * incr).collect())
}

/// The keyword `key` of `record`.
fn keyword<'a>(record: &'a RecordValue, key: &KeywordRef) -> Option<&'a Value> {
    match key {
        KeywordRef::Whole => None,
        KeywordRef::Index(index) => usize::try_from(*index)
            .ok()
            .and_then(|index| record.fields().get(index))
            .map(|field| &field.value),
        KeywordRef::Name(name) => {
            let mut parts = name.split('.');
            let mut found = record.get(parts.next()?)?;
            for part in parts {
                let Value::Record(inner) = found else {
                    return None;
                };
                found = inner.get(part)?;
            }
            Some(found)
        }
    }
}

/// A shape as `getcolshapestring` writes it: `[4, 64]`.
fn shape_text(shape: &[usize]) -> String {
    let parts: Vec<String> = shape.iter().map(ToString::to_string).collect();
    format!("[{}]", parts.join(", "))
}

fn field_names(record: &RecordValue) -> CasaValue {
    texts(record.fields().iter().map(|field| field.name.clone()))
}

impl Open {
    fn column(&self, name: &str, refusal: impl FnOnce() -> String) -> Result<&ColumnSchema, Fail> {
        self.table
            .schema()
            .and_then(|schema| schema.column(name))
            .ok_or_else(|| Fail::Runtime(refusal()))
    }

    /// A column of the table, or CASA's "unknown" refusal.
    fn known(&self, name: &str) -> Result<&ColumnSchema, Fail> {
        self.column(name, || format!("Table column {name} is unknown"))
    }

    fn undefined(row: usize, column: &str) -> Fail {
        Fail::Runtime(format!(
            "Table DataManager error: Invalid operation: no array in row {row} of column {column}"
        ))
    }

    /// The array cells of `rows`, each as casatools returns it.
    fn array_cells(&self, column: &str, rows: &[usize]) -> Result<Vec<CasaValue>, Fail> {
        let cells = self
            .table
            .column_accessor(column)?
            .array_cells_owned_uncached(rows)?;
        cells
            .iter()
            .zip(rows)
            .map(|(cell, row)| {
                cell.as_ref()
                    .map(values::array)
                    .ok_or_else(|| Self::undefined(*row, column))
            })
            .collect()
    }

    /// The scalar cells of `rows`.
    fn scalar_cells(&self, column: &str, rows: &[usize]) -> Result<Vec<ScalarValue>, Fail> {
        let cells = self
            .table
            .column_accessor(column)?
            .scalar_cells_owned_for_rows(rows)?;
        cells
            .into_iter()
            .zip(rows)
            .map(|(cell, row)| {
                cell.ok_or_else(|| {
                    Fail::Runtime(format!(
                        "Table DataManager error: Invalid operation: no value in row {row} of column {column}"
                    ))
                })
            })
            .collect()
    }

    fn getcell(&self, column: &str, row: i64) -> Reply {
        let schema = self.column(column, || {
            format!("TableProxy::getCell: column {column} does not exist")
        })?;
        let row = usize::try_from(row)
            .ok()
            .filter(|row| *row < self.table.row_count())
            .ok_or_else(|| Fail::Runtime("TableProxy::getCell: no such row".into()))?;
        Ok(match schema.column_type() {
            ColumnType::Record => {
                let record = self.table.column_accessor(column)?.record_cell(row)?;
                values::record(&record, &self.name)
            }
            ColumnType::Scalar => {
                let cells = self.scalar_cells(column, &[row])?;
                CasaValue::from_scalar(&cells[0])
            }
            ColumnType::Array(_) => self.array_cells(column, &[row])?.remove(0),
        })
    }

    fn getcol(&self, column: &str, start: i64, count: i64, incr: i64) -> Reply {
        let schema = self.column(column, || {
            format!("TableProxy::getColumn: column {column} does not exist")
        })?;
        let rows = rows(
            self.table.row_count(),
            start,
            count,
            incr,
            "TableProxy::getColumn: no such row",
        )?;
        match schema.column_type() {
            ColumnType::Record => {
                runtime("TableProxy::getColumn not possible for a column containing records".into())
            }
            _ if rows.is_empty() => Ok(values::empty_column(schema.data_type())),
            ColumnType::Scalar => Ok(values::scalar_column(self.scalar_cells(column, &rows)?)),
            ColumnType::Array(_) => values::stack_rows(&self.array_cells(column, &rows)?),
        }
    }

    fn getvarcol(&self, column: &str, start: i64, count: i64, incr: i64) -> Reply {
        let schema = self.column(column, || {
            format!("TableProxy::getVarColumn: column {column} does not exist")
        })?;
        let rows = rows(
            self.table.row_count(),
            start,
            count,
            incr,
            "TableProxy::getVarColumn: no such row",
        )?;
        let cells = match schema.column_type() {
            ColumnType::Record => {
                return runtime(
                    "TableProxy::getColumn not possible for a column containing records".into(),
                );
            }
            ColumnType::Scalar => self
                .scalar_cells(column, &rows)?
                .into_iter()
                .map(|cell| values::scalar_column(vec![cell]))
                .collect(),
            ColumnType::Array(_) => self
                .array_cells(column, &rows)?
                .into_iter()
                .map(|cell| values::stack_rows(&[cell]))
                .collect::<Result<Vec<_>, _>>()?,
        };
        Ok(CasaValue::record(
            rows.iter()
                .zip(cells)
                .map(|(row, cell)| (format!("r{}", row + 1), cell)),
        ))
    }

    fn getcolshapestring(&self, column: &str, start: i64, count: i64, incr: i64) -> Reply {
        let schema = self.column(column, || {
            format!("TableProxy::getColumnShapeString: column {column} does not exist")
        })?;
        let contract = match schema.column_type() {
            ColumnType::Array(contract) => contract,
            _ => {
                return runtime(format!(
                    "Invalid Table operation: invalid shapeColumn() for column {column}; only valid for an array"
                ));
            }
        };
        let rows = rows_stepping(
            self.table.row_count(),
            start,
            count,
            incr,
            "TableProxy::getColumnShapeString: no such row",
            "TableProxy::getColumnShapeString: rowincr<=0",
        )?;
        // A column of one shape has one answer, whatever the rows asked for
        // (once the rows asked for are known to exist).
        if let casa_tables::ArrayShapeContract::Fixed { shape } = contract {
            return Ok(texts([shape_text(shape)]));
        }
        let mut shapes = Vec::with_capacity(rows.len());
        for row in rows {
            let shape = self
                .table
                .array_shape(row, column)?
                .ok_or_else(|| Self::undefined(row, column))?;
            shapes.push(shape_text(&shape));
        }
        Ok(texts(shapes))
    }

    fn iscelldefined(&self, column: &str, row: i64) -> Reply {
        self.known(column)?;
        let nrows = self.table.row_count();
        let row = usize::try_from(row)
            .ok()
            .filter(|row| *row < nrows)
            .ok_or_else(|| {
                Fail::Runtime(format!(
                    "TableColumn: row number {} exceeds #rows {nrows} in table {}",
                    row as u64,
                    self.name.display()
                ))
            })?;
        Ok(CasaValue::Bool(self.table.is_cell_defined(row, column)?))
    }

    fn colarraytype(&self, column: &str) -> Reply {
        let schema = self.known(column)?;
        match schema.column_type() {
            ColumnType::Array(contract) => {
                let direct = if schema.options().direct {
                    "Direct"
                } else {
                    "Indirect"
                };
                let sized = if matches!(contract, casa_tables::ArrayShapeContract::Fixed { .. }) {
                    "fixed"
                } else {
                    "variable"
                };
                Ok(CasaValue::Text(format!("{direct}, {sized} sized arrays")))
            }
            _ => runtime(format!("column {column} is a scalar column")),
        }
    }

    fn keyword_names(record: &RecordValue, key: &KeywordRef, whose: &str) -> Reply {
        match key {
            KeywordRef::Whole => Ok(field_names(record)),
            KeywordRef::Name(name) if name.is_empty() => Ok(field_names(record)),
            _ => match keyword(record, key) {
                Some(Value::Record(inner)) => Ok(field_names(inner)),
                Some(_) => runtime("Keyword does not contain a subrecord".into()),
                None => runtime(format!("{whose} does not exist")),
            },
        }
    }
}

impl State {
    /// `ok()`.
    pub(super) fn ok(&self) -> CasaValue {
        CasaValue::Bool(self.open.is_some())
    }

    /// `name()`.
    pub(super) fn name(&self, log: &mut Vec<LogLine>) -> CasaValue {
        match self.table(log) {
            Some(open) => CasaValue::Text(open.name.to_string_lossy().into_owned()),
            None => CasaValue::Text(String::new()),
        }
    }

    /// `nrows()`.
    pub(super) fn nrows(&self, log: &mut Vec<LogLine>) -> CasaValue {
        CasaValue::Int(
            self.table(log)
                .map_or(0, |open| open.table.row_count() as i64),
        )
    }

    /// `ncols()`.
    pub(super) fn ncols(&self, log: &mut Vec<LogLine>) -> CasaValue {
        let columns = self
            .table(log)
            .and_then(|open| open.table.schema())
            .map_or(0, |schema| schema.columns().len());
        CasaValue::Int(columns as i64)
    }

    /// `colnames()`.
    pub(super) fn colnames(&self, log: &mut Vec<LogLine>) -> CasaValue {
        let names = self
            .table(log)
            .and_then(|open| open.table.schema())
            .map(|schema| {
                schema
                    .columns()
                    .iter()
                    .map(|column| column.name().to_string())
                    .collect::<Vec<_>>()
            });
        texts(names.unwrap_or_default())
    }

    /// `iswritable()`.
    pub(super) fn iswritable(&self, log: &mut Vec<LogLine>) -> CasaValue {
        CasaValue::Bool(self.table(log).is_some_and(|open| open.writable))
    }

    /// `info()`.
    pub(super) fn info(&self, log: &mut Vec<LogLine>) -> CasaValue {
        let Some(open) = self.table(log) else {
            return CasaValue::empty_record();
        };
        let info = open.table.info();
        let readme: String = info.readme.iter().map(|line| format!("{line}\n")).collect();
        CasaValue::record([
            ("readme", CasaValue::Text(readme)),
            ("subType", CasaValue::Text(info.sub_type.clone())),
            ("type", CasaValue::Text(info.table_type.clone())),
        ])
    }

    /// `lockoptions()`.
    pub(super) fn lockoptions(&self, log: &mut Vec<LogLine>) -> CasaValue {
        match self.lock_settings() {
            Some((option, interval, maxwait)) => CasaValue::record([
                ("interval", CasaValue::Float(interval)),
                ("maxwait", CasaValue::Int(maxwait)),
                ("option", CasaValue::Text(option.to_string())),
            ]),
            None => {
                no_table_specified(log);
                CasaValue::empty_record()
            }
        }
    }

    /// `haslock(write)`, with the warning CASA gives with no table open.
    pub(super) fn haslock(&self, write: bool, log: &mut Vec<LogLine>) -> CasaValue {
        if self.open.is_none() {
            no_table_specified(log);
        }
        CasaValue::Bool(self.has_lock(write))
    }

    /// `isscalarcol(columnname)`: a record column counts as scalar.
    pub(super) fn isscalarcol(&self, column: &str, log: &mut Vec<LogLine>) -> Reply {
        let Some(open) = self.table(log) else {
            return Ok(CasaValue::Bool(false));
        };
        let scalar = !matches!(open.known(column)?.column_type(), ColumnType::Array(_));
        Ok(CasaValue::Bool(scalar))
    }

    /// `isvarcol(columnname)`.
    pub(super) fn isvarcol(&self, column: &str, log: &mut Vec<LogLine>) -> Reply {
        let Some(open) = self.table(log) else {
            return Ok(CasaValue::Bool(false));
        };
        let variable = matches!(
            open.known(column)?.column_type(),
            ColumnType::Array(casa_tables::ArrayShapeContract::Variable { .. })
        );
        Ok(CasaValue::Bool(variable))
    }

    /// `coldatatype(columnname)`.
    pub(super) fn coldatatype(&self, column: &str, log: &mut Vec<LogLine>) -> Reply {
        let Some(open) = self.table(log) else {
            return Ok(CasaValue::Text(String::new()));
        };
        Ok(CasaValue::Text(data_type_name(open.known(column)?).into()))
    }

    /// `colarraytype(columnname)`.
    pub(super) fn colarraytype(&self, column: &str, log: &mut Vec<LogLine>) -> Reply {
        let Some(open) = self.table(log) else {
            return Ok(CasaValue::Text(String::new()));
        };
        open.colarraytype(column)
    }

    /// `getcell(columnname, rownr)`.
    pub(super) fn getcell(&self, column: &str, row: i64, log: &mut Vec<LogLine>) -> Reply {
        let Some(open) = self.table(log) else {
            return Ok(empty_array());
        };
        open.getcell(column, row)
    }

    /// `getcol(columnname, startrow, nrow, rowincr)`.
    pub(super) fn getcol(
        &self,
        column: &str,
        rows: (i64, i64, i64),
        log: &mut Vec<LogLine>,
    ) -> Reply {
        let Some(open) = self.table(log) else {
            return Ok(empty_array());
        };
        open.getcol(column, rows.0, rows.1, rows.2)
    }

    /// `getvarcol(columnname, startrow, nrow, rowincr)`.
    pub(super) fn getvarcol(
        &self,
        column: &str,
        rows: (i64, i64, i64),
        log: &mut Vec<LogLine>,
    ) -> Reply {
        let Some(open) = self.table(log) else {
            return Ok(CasaValue::empty_record());
        };
        open.getvarcol(column, rows.0, rows.1, rows.2)
    }

    /// `getcolshapestring(columnname, startrow, nrow, rowincr)`.
    pub(super) fn getcolshapestring(
        &self,
        column: &str,
        rows: (i64, i64, i64),
        log: &mut Vec<LogLine>,
    ) -> Reply {
        let Some(open) = self.table(log) else {
            return Ok(texts([]));
        };
        open.getcolshapestring(column, rows.0, rows.1, rows.2)
    }

    /// `iscelldefined(columnname, rownr)`.
    pub(super) fn iscelldefined(&self, column: &str, row: i64, log: &mut Vec<LogLine>) -> Reply {
        let Some(open) = self.table(log) else {
            return Ok(CasaValue::Bool(false));
        };
        open.iscelldefined(column, row)
    }

    /// `keywordnames()`.
    pub(super) fn keywordnames(&self, log: &mut Vec<LogLine>) -> CasaValue {
        self.table(log)
            .map_or_else(|| texts([]), |open| field_names(open.table.keywords()))
    }

    /// `getkeywords()`.
    pub(super) fn getkeywords(&self, log: &mut Vec<LogLine>) -> CasaValue {
        match self.table(log) {
            Some(open) => values::record(open.table.keywords(), &open.name),
            None => CasaValue::empty_record(),
        }
    }

    /// `getkeyword(keyword)`.
    pub(super) fn getkeyword(&self, key: &KeywordRef, log: &mut Vec<LogLine>) -> Reply {
        let Some(open) = self.table(log) else {
            return Ok(empty_array());
        };
        match keyword(open.table.keywords(), key) {
            Some(found) => Ok(values::value(found, &open.name)),
            None => match key {
                KeywordRef::Whole => Ok(empty_array()),
                KeywordRef::Name(name) if !name.is_empty() => {
                    runtime(format!("Table keyword {name} does not exist"))
                }
                _ => runtime(NO_SUCH_FIELD.into()),
            },
        }
    }

    /// `fieldnames(keyword)`.
    pub(super) fn fieldnames(&self, key: &KeywordRef, log: &mut Vec<LogLine>) -> Reply {
        let Some(open) = self.table(log) else {
            return Ok(texts([]));
        };
        let whose = match key {
            KeywordRef::Whole => String::new(),
            KeywordRef::Name(name) => format!("Table keyword {name}"),
            KeywordRef::Index(index) => format!("Table keyword {index}"),
        };
        Open::keyword_names(open.table.keywords(), key, &whose)
    }

    /// `colkeywordnames(columnname)`.
    pub(super) fn colkeywordnames(&self, column: &str, log: &mut Vec<LogLine>) -> Reply {
        let Some(open) = self.table(log) else {
            return Ok(texts([]));
        };
        open.known(column)?;
        Ok(open
            .table
            .column_keywords(column)
            .map_or_else(|| texts([]), field_names))
    }

    /// `getcolkeywords(columnname)`.
    pub(super) fn getcolkeywords(&self, column: &str, log: &mut Vec<LogLine>) -> Reply {
        let Some(open) = self.table(log) else {
            return Ok(empty_array());
        };
        open.known(column)?;
        Ok(open
            .table
            .column_keywords(column)
            .map_or_else(CasaValue::empty_record, |record| {
                values::record(record, &open.name)
            }))
    }

    /// `getcolkeyword(columnname, keyword)`.
    pub(super) fn getcolkeyword(
        &self,
        column: &str,
        key: &KeywordRef,
        log: &mut Vec<LogLine>,
    ) -> Reply {
        let Some(open) = self.table(log) else {
            return Ok(empty_array());
        };
        open.known(column)?;
        let found = open
            .table
            .column_keywords(column)
            .and_then(|record| keyword(record, key));
        match found {
            Some(found) => Ok(values::value(found, &open.name)),
            None => match key {
                KeywordRef::Whole => Ok(empty_array()),
                KeywordRef::Name(name) => {
                    runtime(format!("Keyword {name} in column {column} does not exist"))
                }
                KeywordRef::Index(_) => runtime(
                    "TableRecordRep: Failed AlwaysAssert whichField >= 0 && whichField < Int(nused_p)"
                        .into(),
                ),
            },
        }
    }

    /// `colfieldnames(columnname, keyword)`.
    pub(super) fn colfieldnames(
        &self,
        column: &str,
        key: &KeywordRef,
        log: &mut Vec<LogLine>,
    ) -> Reply {
        let Some(open) = self.table(log) else {
            return Ok(texts([]));
        };
        open.known(column)?;
        let empty = RecordValue::new(Vec::new());
        let record = open.table.column_keywords(column).unwrap_or(&empty);
        Open::keyword_names(record, key, &format!("Keyword in column {column}"))
    }
}
