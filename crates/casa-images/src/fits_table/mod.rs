// SPDX-License-Identifier: LGPL-3.0-or-later
//! Convert binary or ASCII FITS extensions to native casacore tables.

mod cards;
mod column;
pub mod listing;

use std::path::Path;

use casa_core::{RecordField, RecordValue, Value};
use casa_tables::{Table, TableSchema};
use fitsio::{FitsFile, sys};

use cards::Header;
use column::Column;

const INVALID_HDU: &str =
    "The indicated FITS file does not have a valid binary table at HDU requested";

/// Failure to read a FITS table or represent it in a casacore table.
#[derive(Debug, thiserror::Error)]
pub enum FitsTableError {
    /// FITS data could not be read.
    #[error("{0}")]
    Fits(String),
    /// The derived table schema is not valid.
    #[error(transparent)]
    Schema(#[from] casa_tables::SchemaError),
    /// A converted table row violates the schema.
    #[error(transparent)]
    Table(#[from] casa_tables::TableError),
}

type Result<T> = std::result::Result<T, FitsTableError>;

fn fits_status(status: i32) -> Result<()> {
    fitsio::errors::check_status(status).map_err(|error| FitsTableError::Fits(error.to_string()))
}

/// Read the requested FITS table extension (the first extension is HDU 1).
///
/// Reads table values, units, display formats and null markers, FITS
/// keywords, and fixed or per-row array shapes. With `sdfits`, header
/// keywords except the reserved table-wide fields become constant
/// columns, and the TDIM pseudo-columns are consumed but not emitted.
pub fn read_fits_table(path: &Path, hdu: usize, sdfits: bool) -> Result<Table> {
    if hdu == 0 {
        return Err(FitsTableError::Fits(
            "whichHDU is not valid, must be >= 1".into(),
        ));
    }
    if std::fs::File::open(path).is_err() {
        return Err(FitsTableError::Fits(format!(
            "{} is not readable",
            path.display()
        )));
    }
    let invalid_hdu = || FitsTableError::Fits(INVALID_HDU.into());
    let mut file = FitsFile::open(path).map_err(|_| invalid_hdu())?;
    let index = i32::try_from(hdu)
        .ok()
        .and_then(|index| index.checked_add(1))
        .ok_or_else(invalid_hdu)?;
    // SAFETY: FitsFile owns this pointer through the conversion. The raw
    // calls avoid fitsio's high-level schema parser, which lacks heap and
    // complex column types.
    let ptr = unsafe { file.as_raw() };
    let (mut kind, mut num_rows, mut status) = (0, 0, 0);
    // SAFETY: Ptr has exclusive use here and the output pointers are live.
    unsafe {
        sys::ffmahd(ptr, index, &mut kind, &mut status);
        sys::ffgnrwll(ptr, &mut num_rows, &mut status);
    }
    if status != 0 || (kind as u32 != sys::ASCII_TBL && kind as u32 != sys::BINARY_TBL) {
        return Err(invalid_hdu());
    }
    let num_rows = usize::try_from(num_rows).map_err(|_| invalid_hdu())?;
    let mut fits = Reader {
        ptr,
        ascii_table: kind as u32 == sys::ASCII_TBL,
    };
    let header = Header::read(&mut fits)?;
    let columns = Column::read(&mut fits, &header)?;
    convert(&mut fits, &header, &columns, num_rows, sdfits)
}

struct Reader {
    ptr: *mut sys::fitsfile,
    ascii_table: bool,
}

fn convert(
    fits: &mut Reader,
    header: &Header,
    cols: &[Column],
    rows: usize,
    sdfits: bool,
) -> Result<Table> {
    let mut columns = Vec::new();
    let mut sources = Vec::new();
    let mut keywords = RecordValue::default();
    let mut constants = Vec::new();
    for col in cols {
        if !sdfits || !col.name.starts_with("TDIM") {
            columns.push(col.schema()?);
            sources.push(col);
        }
    }
    for card in header.keywords() {
        let Some(value) = &card.scalar else { continue };
        if sdfits && card.is_column() && !cols.iter().any(|column| column.name == card.name) {
            columns.push(
                casa_tables::ColumnSchema::scalar(&card.name, value.primitive_type())
                    .with_comment(&card.comment),
            );
            constants.push(RecordField::new(&card.name, Value::Scalar(value.clone())));
        } else {
            keywords.upsert(&card.name, Value::Scalar(value.clone()));
        }
    }
    let mut table = Table::with_schema(TableSchema::new(columns)?);
    *table.keywords_mut() = keywords;
    for column in &sources {
        let keywords = column.keywords(header);
        if !keywords.fields().is_empty() {
            table.set_column_keywords(&column.name, keywords);
        }
    }
    for row in 1..=rows {
        let mut fields = Vec::with_capacity(sources.len() + constants.len());
        for column in &sources {
            fields.push(RecordField::new(
                &column.name,
                column.value(fits, cols, row as i64)?,
            ));
        }
        fields.extend(constants.iter().cloned());
        table.add_row(RecordValue::new(fields))?;
    }
    Ok(table)
}
