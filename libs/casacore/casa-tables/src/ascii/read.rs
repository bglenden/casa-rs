// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use casa_core::quanta::{Quantity, casacore::mvangle_radians};
use casa_core::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use num_complex::{Complex32, Complex64};
use regex::Regex;

use super::AsciiError;
use crate::{ColumnOptions, ColumnSchema, Table, TableSchema};

/// Options for importing a casacore ASCII file.
#[derive(Debug, Clone)]
pub struct AsciiInput<'a> {
    /// A separate header file, or the data file itself if omitted.
    pub header: Option<&'a Path>,
    /// Infer column types and names from the first row.
    pub auto_header: bool,
    /// Field separator (exactly one ASCII byte).
    pub separator: &'a str,
    /// Ignore lines matched at the beginning by this regular expression.
    pub comment_marker: &'a str,
    /// First physical data line, numbered from one; zero means the beginning.
    pub first_line: i64,
    /// Last physical data line, inclusive; nonpositive means through EOF.
    pub last_line: i64,
    /// Names supplied in lieu of a header.
    pub column_names: &'a [String],
    /// Types supplied with the column names.
    pub data_types: &'a [String],
}

type ReadResult<T> = Result<T, AsciiError>;

fn format_error(message: impl Into<String>) -> AsciiError {
    AsciiError::Format(format!("ReadAsciiTable: {}", message.into()))
}

fn lines<'a>(
    path: &'a Path,
    first: i64,
    last: i64,
    comment: Option<&'a Regex>,
) -> ReadResult<impl Iterator<Item = ReadResult<String>> + 'a> {
    let io_error = |source| AsciiError::Io {
        path: path.into(),
        source,
    };
    let first = usize::try_from(first.max(1) - 1).unwrap_or(usize::MAX);
    let end = if last <= 0 {
        usize::MAX
    } else {
        usize::try_from(last).unwrap_or(usize::MAX)
    };
    let file = BufReader::new(File::open(path).map_err(io_error)?);
    Ok(file
        .lines()
        .take(end)
        .skip(first)
        .filter_map(move |line| match line {
            Ok(line)
                if comment
                    .and_then(|re| re.find(&line))
                    .is_some_and(|m| m.start() == 0) =>
            {
                None
            }
            other => Some(other.map_err(io_error)),
        }))
}

struct Fields<'a> {
    input: &'a str,
    separator: char,
    pending_empty: bool,
}

impl<'a> Fields<'a> {
    fn new(input: &'a str, separator: char) -> Self {
        Self {
            input,
            separator,
            pending_empty: false,
        }
    }
}

impl Iterator for Fields<'_> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        let (mut found, mut quoted) = (false, false);
        let mut field = String::new();
        for (offset, ch) in self.input.char_indices() {
            match ch {
                '"' => {
                    quoted = !quoted;
                    found = true;
                }
                _ if quoted => field.push(ch),
                _ => {
                    let ch = if ch == '\t' { ' ' } else { ch };
                    if ch == self.separator && (found || self.separator != ' ') {
                        self.input = &self.input[offset + ch.len_utf8()..];
                        self.pending_empty = ch != ' ';
                        return Some(field);
                    }
                    if ch != ' ' {
                        field.push(ch);
                        found = true;
                    }
                }
            }
        }
        self.input = "";
        (found | std::mem::take(&mut self.pending_empty)).then_some(field)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Encoding {
    Plain,
    Polar,
    Dms,
    Hms,
}

struct DataType {
    primitive: PrimitiveType,
    encoding: Encoding,
    shape: Vec<usize>,
    variable_axis: Option<usize>,
}

impl DataType {
    fn parse(spec: &str) -> ReadResult<Self> {
        use Encoding::*;
        use PrimitiveType::*;
        let spec = spec.to_ascii_uppercase();
        let end_type = spec
            .find(|ch: char| ch.is_ascii_digit())
            .unwrap_or(spec.len());
        if spec[..end_type].contains('[') {
            return Err(format_error(format!(
                "invalid bracketed type string '{spec}'"
            )));
        }
        let (name, dimensions) = spec.split_at(end_type);
        let (primitive, encoding) = match name {
            "B" => (Bool, Plain),
            "S" => (Int16, Plain),
            "I" => (Int32, Plain),
            "R" => (Float32, Plain),
            "D" => (Float64, Plain),
            "DMS" => (Float64, Dms),
            "HMS" => (Float64, Hms),
            "A" => (String, Plain),
            "X" => (Complex32, Plain),
            "Z" => (Complex32, Polar),
            "DX" => (Complex64, Plain),
            "DZ" => (Complex64, Polar),
            _ => return Err(format_error(format!("invalid type specifier '{name}'"))),
        };
        let mut shape = Vec::new();
        let mut variable_axis = None;
        if !dimensions.is_empty() {
            for dim in dimensions.split(',') {
                let extent = dim.parse::<usize>().map_err(|_| {
                    format_error(format!(
                        "invalid shape value '{dim}' in type string '{spec}'"
                    ))
                })?;
                if extent == 0 && variable_axis.replace(shape.len()).is_some() {
                    return Err(format_error(format!(
                        "multiple variable axes in type string '{spec}'"
                    )));
                }
                shape.push(extent.max(1));
            }
        }
        Ok(Self {
            primitive,
            encoding,
            shape,
            variable_axis,
        })
    }

    fn column(&self, name: &str) -> ReadResult<ColumnSchema> {
        Ok(if self.shape.is_empty() {
            ColumnSchema::scalar(name, self.primitive)
        } else if self.variable_axis.is_some() {
            ColumnSchema::array_variable(name, self.primitive, None)
        } else {
            ColumnSchema::array_fixed(name, self.primitive, self.shape.clone()).with_options(
                ColumnOptions {
                    direct: true,
                    undefined: false,
                },
            )?
        })
    }

    fn value(&self, fields: &mut Fields<'_>, keyword: bool) -> ReadResult<Value> {
        if self.shape.is_empty() && !keyword {
            return Ok(Value::Scalar(
                self.scalar(fields.next().unwrap_or_default(), fields),
            ));
        }
        let mut shape = if self.shape.is_empty() {
            vec![1]
        } else {
            self.shape.clone()
        };
        let axis = if self.shape.is_empty() {
            Some(0)
        } else {
            self.variable_axis
        };
        let size = shape
            .iter()
            .try_fold(1usize, |n, d| n.checked_mul(*d))
            .ok_or_else(|| format_error("array shape is too large"))?;
        let mut values = Vec::new();
        while axis.is_some() || values.len() < size {
            let Some(field) = fields.next() else { break };
            values.push(self.scalar(field, fields));
        }
        if let Some(axis) = axis {
            shape[axis] = values.len().div_ceil(size);
        }
        if keyword && self.shape.is_empty() && values.len() == 1 {
            return Ok(Value::Scalar(values.pop().expect("one value")));
        }
        let padded = size
            .checked_mul(axis.map_or(1, |i| shape[i]))
            .ok_or_else(|| format_error("array shape is too large"))?;
        values
            .try_reserve(padded.saturating_sub(values.len()))
            .map_err(|_| format_error("array shape is too large"))?;
        values.resize_with(padded, || {
            self.scalar(String::new(), &mut Fields::new("", ' '))
        });
        Ok(Value::Array(make_array(self.primitive, &shape, values)?))
    }

    fn scalar(&self, text: String, fields: &mut Fields<'_>) -> ScalarValue {
        use ScalarValue as S;
        match self.primitive {
            PrimitiveType::Bool => {
                S::Bool(!text.is_empty() && text != "0" && !text.starts_with(['F', 'f', 'N', 'n']))
            }
            PrimitiveType::Int16 => {
                S::Int16(parse_int(&text).clamp(i16::MIN.into(), i16::MAX.into()) as i16)
            }
            PrimitiveType::Int32 => {
                S::Int32(parse_int(&text).clamp(i32::MIN.into(), i32::MAX.into()) as i32)
            }
            PrimitiveType::Float32 => S::Float32(parse_ascii(&text)),
            PrimitiveType::Float64 => {
                S::Float64(if matches!(self.encoding, Encoding::Dms | Encoding::Hms) {
                    angle(&text, self.encoding == Encoding::Dms)
                } else {
                    parse_ascii(&text)
                })
            }
            PrimitiveType::String => S::String(text),
            PrimitiveType::Complex32 => {
                let first: f32 = parse_ascii(&text);
                let second: f32 = parse_ascii(&fields.next().unwrap_or_default());
                S::Complex32(if self.encoding == Encoding::Polar {
                    let radians = (f64::from(second) * CASA_DEGREE) as f32;
                    Complex32::from_polar(first, radians)
                } else {
                    Complex32::new(first, second)
                })
            }
            PrimitiveType::Complex64 => {
                let first: f64 = parse_ascii(&text);
                let second: f64 = parse_ascii(&fields.next().unwrap_or_default());
                S::Complex64(if self.encoding == Encoding::Polar {
                    Complex64::from_polar(first, second * CASA_DEGREE)
                } else {
                    Complex64::new(first, second)
                })
            }
            _ => unreachable!("primitive not used by ASCII table columns"),
        }
    }
}

// ReadAsciiTable uses this decimal approximation for a polar complex value.
const CASA_DEGREE: f64 = 314_159_265.0 / 100_000_000.0 / 180.0;

fn parse_ascii<T: std::str::FromStr + Default>(text: &str) -> T {
    if text
        .as_bytes()
        .iter()
        .all(|b| matches!(b, b'0'..=b'9' | b'+' | b'-' | b'.' | b'e' | b'E'))
    {
        text.parse().unwrap_or_default()
    } else {
        T::default()
    }
}

fn parse_int(text: &str) -> i64 {
    let (sign, digits) = match text.as_bytes().first() {
        Some(b'-') => (-1_i64, &text.as_bytes()[1..]),
        Some(b'+') => (1_i64, &text.as_bytes()[1..]),
        _ => (1_i64, text.as_bytes()),
    };
    digits
        .iter()
        .copied()
        .take_while(u8::is_ascii_digit)
        .fold(0_i64, |acc, b| {
            acc.saturating_mul(10)
                .saturating_add(sign * i64::from(b - b'0'))
        })
}

fn angle(text: &str, dms: bool) -> f64 {
    // ReadAsciiTable::stringToPos tolerates whitespace around units and
    // accepts spaces between unmarked sexagesimal components.
    let mut input = String::with_capacity(text.len());
    let mut blank = false;
    let mut needs_value = true;
    for ch in text.trim().chars().map(|ch| ch.to_ascii_lowercase()) {
        if ch.is_ascii_whitespace() {
            blank = true;
            continue;
        }
        if matches!(ch, ':' | '.' | 'h' | 'd' | 'm') {
            if needs_value {
                input.push('0');
            }
            needs_value = true;
        } else if !matches!(ch, '+' | '-') {
            if !needs_value && blank {
                input.push(':');
            }
            needs_value = false;
            blank = false;
        }
        input.push(ch);
    }
    let radians = input.parse::<f64>().unwrap_or_else(|_| {
        Quantity::read(&input)
            .ok()
            .and_then(|q| mvangle_radians(&q).ok())
            .unwrap_or(0.0)
    });
    if dms && input.contains(':') {
        radians / 15.0
    } else {
        radians
    }
}

fn make_array(
    primitive: PrimitiveType,
    shape: &[usize],
    values: Vec<ScalarValue>,
) -> ReadResult<ArrayValue> {
    macro_rules! build {
        ($variant:ident) => {{
            let elements = values
                .into_iter()
                .map(|value| match value {
                    ScalarValue::$variant(v) => v,
                    _ => unreachable!("ASCII column changes type"),
                })
                .collect::<Vec<_>>();
            ArrayD::from_shape_vec(IxDyn(shape).f(), elements).map(ArrayValue::$variant)
        }};
    }
    match primitive {
        PrimitiveType::Bool => build!(Bool),
        PrimitiveType::Int16 => build!(Int16),
        PrimitiveType::Int32 => build!(Int32),
        PrimitiveType::Float32 => build!(Float32),
        PrimitiveType::Float64 => build!(Float64),
        PrimitiveType::Complex32 => build!(Complex32),
        PrimitiveType::Complex64 => build!(Complex64),
        PrimitiveType::String => build!(String),
        _ => unreachable!("primitive not used by ASCII table columns"),
    }
    .map_err(|error| format_error(error.to_string()))
}

fn header_fields(line: &str, separator: char) -> Vec<String> {
    let separator = if line.contains(separator) {
        separator
    } else {
        ' '
    };
    Fields::new(line, separator).collect()
}

fn infer(line: &str, separator: char) -> (Vec<String>, Vec<String>) {
    Fields::new(line, separator)
        .enumerate()
        .map(|(i, field)| {
            let kind = if field.parse::<i64>().is_ok() {
                "I"
            } else if field.parse::<f64>().is_ok_and(f64::is_finite) {
                "D"
            } else {
                "A"
            };
            (format!("Column{}", i + 1), kind.into())
        })
        .unzip()
}

fn keyword_sets(
    mut line: String,
    lines: &mut impl Iterator<Item = ReadResult<String>>,
    separator: char,
) -> ReadResult<(String, BTreeMap<String, RecordValue>)> {
    let mut sets = BTreeMap::new();
    while line.starts_with(".key") {
        let column = Fields::new(&line, ' ').nth(1).unwrap_or_default();
        let mut record = RecordValue::default();
        loop {
            let line = lines
                .next()
                .transpose()?
                .ok_or_else(|| format_error("no .endkey line"))?;
            if line.starts_with(".endkey") {
                break;
            }
            let mut fields = Fields::new(&line, ' ');
            let name = fields
                .next()
                .ok_or_else(|| format_error("no keyword name"))?;
            let kind = fields
                .next()
                .ok_or_else(|| format_error("no keyword type"))?;
            if record.get(&name).is_none() {
                fields.separator = separator;
                let value = DataType::parse(&kind)?.value(&mut fields, true)?;
                record.push(RecordField::new(name, value));
            }
        }
        sets.entry(column).or_insert(record);
        line = lines.next().transpose()?.unwrap_or_default();
    }
    Ok((line, sets))
}

fn populate(
    mut table: Table,
    names: &[String],
    types: &[DataType],
    mut keys: BTreeMap<String, RecordValue>,
    data: impl Iterator<Item = ReadResult<String>>,
    separator: char,
) -> ReadResult<Table> {
    if let Some(record) = keys.remove("") {
        *table.keywords_mut() = record;
    }
    for (name, kind) in names.iter().zip(types) {
        let mut record = keys.remove(name).unwrap_or_default();
        if !kind.shape.is_empty() && matches!(kind.encoding, Encoding::Dms | Encoding::Hms) {
            record.upsert(
                "QuantumUnits",
                Value::Array(ArrayValue::from_string_vec(vec!["rad".into()])),
            );
        }
        if !record.fields().is_empty() {
            table.set_column_keywords(name, record);
        }
    }
    for line in data {
        let line = line?;
        let mut fields = Fields::new(&line, separator);
        let row = names
            .iter()
            .zip(types)
            .map(|(name, kind)| Ok(RecordField::new(name, kind.value(&mut fields, false)?)))
            .collect::<ReadResult<Vec<_>>>()?;
        table.add_row(RecordValue::new(row))?;
    }
    Ok(table)
}

/// Parse an ASCII table into a native table with typed rows and keyword sets.
///
/// Reads data in physical line order. A variable array axis is accepted in the
/// final column and is sized from the values left on each line. The caller
/// chooses the output location and storage manager when saving the result.
pub fn read_ascii(path: &Path, options: &AsciiInput<'_>) -> ReadResult<Table> {
    let mut chars = options.separator.chars();
    let separator = chars
        .next()
        .filter(|c| c.is_ascii() && chars.next().is_none())
        .ok_or_else(|| AsciiError::Format("tablefromascii : separator must be 1 char".into()))?;
    let comment = (!options.comment_marker.is_empty())
        .then(|| Regex::new(options.comment_marker))
        .transpose()
        .map_err(|error| format_error(error.to_string()))?;
    let header = options.header.unwrap_or(path);
    let separate = header != path;
    let explicit_names = options.column_names;
    let explicit_types = options.data_types;
    let explicit =
        options.header.is_none() && !(explicit_names.is_empty() && explicit_types.is_empty());
    let infer_types = options.header.is_none() && !explicit && options.auto_header;
    let (start, end) = if separate {
        (1, -1)
    } else {
        (options.first_line, options.last_line)
    };
    let mut header_lines = lines(header, start, end, comment.as_ref())?;
    let first = header_lines.next().transpose()?.ok_or_else(|| {
        format_error(format!(
            "cannot read first header line of {}",
            header.display()
        ))
    })?;
    let (first, keys) = keyword_sets(first, &mut header_lines, separator)?;
    let (names, specs, first_data) = if explicit {
        (explicit_names.to_vec(), explicit_types.to_vec(), true)
    } else if infer_types {
        let (names, types) = infer(&first, separator);
        (names, types, true)
    } else {
        if first.is_empty() {
            return Err(format_error(format!(
                "no COLUMN NAMES line in {}",
                header.display()
            )));
        }
        let second = header_lines
            .next()
            .transpose()?
            .ok_or_else(|| format_error(format!("no COLUMN TYPES line in {}", header.display())))?;
        (
            header_fields(&first, separator),
            header_fields(&second, separator),
            false,
        )
    };
    if names.len() != specs.len() || names.iter().chain(&specs).any(String::is_empty) {
        return Err(if explicit {
            format_error("vector of columnNames and dataTypes should have equal length")
        } else {
            format_error(format!(
                "mismatching COLUMN NAMES and TYPES lines in {}",
                header.display()
            ))
        });
    }
    let types: Vec<_> = specs
        .iter()
        .map(|spec| DataType::parse(spec))
        .collect::<ReadResult<_>>()?;
    if types
        .iter()
        .rev()
        .skip(1)
        .any(|kind| kind.variable_axis.is_some())
    {
        return Err(format_error(
            "only last column can have variable shaped arrays",
        ));
    }
    let columns = names
        .iter()
        .zip(&types)
        .map(|(name, kind)| kind.column(name))
        .collect::<ReadResult<_>>()?;
    let table = Table::with_schema(TableSchema::new(columns)?);
    if separate {
        populate(
            table,
            &names,
            &types,
            keys,
            lines(
                path,
                options.first_line,
                options.last_line,
                comment.as_ref(),
            )?,
            separator,
        )
    } else {
        let saved = (first_data && !first.is_empty()).then_some(Ok(first));
        populate(
            table,
            &names,
            &types,
            keys,
            saved.into_iter().chain(header_lines),
            separator,
        )
    }
}
