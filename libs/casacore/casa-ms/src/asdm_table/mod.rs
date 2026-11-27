// SPDX-License-Identifier: LGPL-3.0-or-later
//! Import CASA's legacy translation of individual ASDM XML tables.
//!
//! This is the `table.fromASDM` operation, not conversion of an ASDM
//! dataset or its binary payload to a MeasurementSet.

mod fields;

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use casa_core::{RecordField, RecordValue};
use casa_tables::{ColumnSchema, Table, TableSchema};
use quick_xml::events::{BytesRef, BytesStart, Event};
use quick_xml::{Reader, XmlVersion};

use fields::{Field, read_schema};

/// Errors from decoding one ASDM XML table.
#[derive(Debug, thiserror::Error)]
pub enum AsdmTableError {
    /// The local XML file could not be opened.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// An XML token is not valid.
    #[error(transparent)]
    Xml(#[from] quick_xml::Error),
    /// The file does not describe a supported ASDM table and row values.
    #[error("ASDM XML: {0}")]
    Format(String),
    /// The ASDM schema does not describe a valid casacore table.
    #[error(transparent)]
    Schema(#[from] casa_tables::SchemaError),
    /// An ASDM row cannot be represented in a casacore table.
    #[error(transparent)]
    Table(#[from] casa_tables::TableError),
}

type Result<T> = std::result::Result<T, AsdmTableError>;

/// Decode a single ASDM XML file into a table with CASA's legacy schema.
///
/// Accepts a recognized ASDM table root or the `ASDM` manifest. Resolves
/// standard XML escaping and embedded `EntityRef` fields but does not
/// load DTDs, external entities, binary sidecars, or referenced files.
/// The input XML is read incrementally, one row at a time.
pub fn read_asdm_table(path: &Path) -> Result<Table> {
    import(&mut Reader::from_reader(BufReader::new(File::open(path)?)))
}

fn import(reader: &mut Reader<impl BufRead>) -> Result<Table> {
    let mut buffer = Vec::new();
    let mut document = None;
    loop {
        match reader.read_event_into(&mut buffer)? {
            Event::Start(tag) if document.is_none() => {
                let name = name_of(&tag);
                document = Some(parse_table(reader, &name, &mut buffer)?);
            }
            Event::Empty(tag) if document.is_none() => {
                let cols = read_schema(&name_of(&tag))?
                    .iter()
                    .map(Field::column)
                    .collect();
                document = Some(Table::with_schema(TableSchema::new(cols)?));
            }
            Event::Eof => return document.ok_or_else(|| invalid("no table element")),
            Event::Comment(_) | Event::Decl(_) | Event::PI(_) => {}
            Event::Text(text) if text.as_ref().trim().is_empty() => {}
            _ => return Err(invalid("unexpected content outside the table element")),
        }
        buffer.clear();
    }
}

fn parse_table(
    reader: &mut Reader<impl BufRead>,
    root: &str,
    buffer: &mut Vec<u8>,
) -> Result<Table> {
    let fields = read_schema(root)?;
    let columns = fields
        .iter()
        .map(Field::column)
        .collect::<Vec<ColumnSchema>>();
    let mut table = Table::with_schema(TableSchema::new(columns)?);
    let row_tag = if root == "ASDM" { "Table" } else { "row" };
    loop {
        buffer.clear();
        match reader.read_event_into(buffer)? {
            Event::Start(tag) if tag.local_name().as_ref() == row_tag => {
                let row = parse_row(reader, &fields, buffer)?;
                table.add_row(row)?;
            }
            Event::Empty(tag) if tag.local_name().as_ref() == row_tag => {
                table.add_row(complete_row(&fields, Vec::new()))?;
            }
            Event::Start(tag) => {
                let tag = tag.name().into_inner().to_owned();
                let mut skip = Vec::new();
                reader.read_to_end_into(quick_xml::name::QName(&tag), &mut skip)?;
            }
            Event::End(name) if name.local_name().as_ref() == root => return Ok(table),
            Event::Eof => return Err(invalid(format!("unclosed {root}"))),
            Event::DocType(_) => return Err(invalid("DTD is not supported")),
            _ => {}
        }
    }
}

fn parse_row(
    reader: &mut Reader<impl BufRead>,
    fields: &[Field<'_>],
    buffer: &mut Vec<u8>,
) -> Result<RecordValue> {
    let mut values = Vec::new();
    loop {
        buffer.clear();
        match reader.read_event_into(buffer)? {
            Event::Start(tag) => {
                let name = name_of(&tag);
                if let Some(field) = fields.iter().find(|field| field.name == name) {
                    let text = read_field(reader, buffer)?;
                    if values.iter().any(|v: &RecordField| v.name == name) {
                        return Err(invalid(format!("duplicate column {name} in row")));
                    }
                    values.push(RecordField::new(name, field.value(&text)?));
                } else {
                    let tag = tag.name().into_inner().to_owned();
                    let mut skip = Vec::new();
                    reader.read_to_end_into(quick_xml::name::QName(&tag), &mut skip)?;
                }
            }
            Event::End(_) => return Ok(complete_row(fields, values)),
            Event::Eof => return Err(invalid("unclosed row")),
            Event::DocType(_) => return Err(invalid("DTD is not supported")),
            _ => {}
        }
    }
}

fn complete_row(fields: &[Field<'_>], values: Vec<RecordField>) -> RecordValue {
    let mut row = RecordValue::new(values);
    for field in fields {
        if row.get(field.name).is_none()
            && let Some(value) = field.scalar_default()
        {
            row.push(RecordField::new(field.name, value));
        }
    }
    row
}

fn read_field(reader: &mut Reader<impl BufRead>, buffer: &mut Vec<u8>) -> Result<String> {
    let mut value = String::new();
    loop {
        buffer.clear();
        match reader.read_event_into(buffer)? {
            Event::Text(text) => value.push_str(&text.xml_content(XmlVersion::Implicit1_0)),
            Event::CData(text) => value.push_str(&text.xml_content(XmlVersion::Implicit1_0)),
            Event::GeneralRef(reference) => entity(&reference, &mut value)?,
            Event::Start(tag) | Event::Empty(tag) if tag.local_name().as_ref() == "EntityRef" => {
                value.push_str(&entity_ref(&tag)?);
            }
            Event::End(tag) if tag.local_name().as_ref() == "EntityRef" => {}
            Event::End(_) => return Ok(value),
            Event::Comment(_) => {}
            _ => return Err(invalid("unexpected nested XML in field")),
        }
    }
}

fn entity(reference: &BytesRef<'_>, value: &mut String) -> Result<()> {
    if let Some(ch) = reference.resolve_char_ref()? {
        value.push(ch);
    } else {
        let name = reference.as_ref();
        value.push_str(
            quick_xml::escape::resolve_xml_entity(name)
                .ok_or_else(|| invalid(format!("unknown entity &{name};")))?,
        );
    }
    Ok(())
}

fn entity_ref(tag: &BytesStart<'_>) -> Result<String> {
    let mut values = [String::new(), String::new(), String::new(), String::new()];
    for attr in tag.attributes() {
        let attr = attr.map_err(|error| invalid(error.to_string()))?;
        let i = match attr.key.local_name().as_ref() {
            "entityId" => 0,
            "partId" => 1,
            "entityTypeName" => 2,
            "documentVersion" => 3,
            _ => continue,
        };
        values[i] = attr.normalized_value(XmlVersion::Implicit1_0)?.into_owned();
    }
    Ok(values.join("   "))
}

fn name_of(tag: &BytesStart<'_>) -> String {
    tag.local_name().into_inner().to_owned()
}

fn invalid(message: impl Into<String>) -> AsdmTableError {
    AsdmTableError::Format(message.into())
}
