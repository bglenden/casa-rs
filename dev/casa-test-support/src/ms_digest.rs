// SPDX-License-Identifier: LGPL-3.0-or-later
//! The Rust side of the whole-MeasurementSet digest that C++ casacore also
//! computes ([`MeasurementSetOracle::digest_manifest`]).
//!
//! The manifest covers the main table, the standard MS subtables, table and
//! column keywords, and every row value, plus any table reachable through a
//! `TpTable` keyword. Each table contributes compact digests rather than a
//! textual dump, so two manifests are equal exactly when C++ and Rust read
//! the same schema, keywords and cells. The tables are opened with
//! `casa-tables` alone, as subtables named by the main table's keywords, so
//! any crate can compare a MeasurementSet it wrote with what casacore reads.
//!
//! [`MeasurementSetOracle::digest_manifest`]: crate::ms_interop::MeasurementSetOracle::digest_manifest

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use casa_tables::{ColumnType, Table, TableOptions};
use casa_types::{ArrayValue, PrimitiveType, RecordValue, ScalarValue, Value};

/// The subtables every MeasurementSet has, in manifest order.
const REQUIRED_SUBTABLES: [&str; 12] = [
    "ANTENNA",
    "DATA_DESCRIPTION",
    "FEED",
    "FIELD",
    "FLAG_CMD",
    "HISTORY",
    "OBSERVATION",
    "POINTING",
    "POLARIZATION",
    "PROCESSOR",
    "SPECTRAL_WINDOW",
    "STATE",
];

/// The optional subtables, digested when the main table names them.
const OPTIONAL_SUBTABLES: [&str; 5] = ["DOPPLER", "FREQ_OFFSET", "SOURCE", "SYSCAL", "WEATHER"];

/// The digest manifest of the MeasurementSet at `ms_path`, line for line as
/// the C++ oracle writes it.
///
/// # Panics
///
/// A table that cannot be opened or a required subtable that is missing.
pub fn measurement_set_manifest(ms_path: &Path) -> String {
    let root_path = normalize_existing_path(ms_path);
    let main = open_table(&root_path);
    let mut visited = BTreeSet::new();
    let mut manifest = Vec::new();
    digest_table_recursive(
        &main,
        "MAIN".to_string(),
        &root_path,
        &mut visited,
        &mut manifest,
    );
    for label in REQUIRED_SUBTABLES {
        let table = subtable(&main, &root_path, label)
            .unwrap_or_else(|| panic!("MeasurementSet has no {label} subtable"));
        digest_table_recursive(
            &table,
            label.to_string(),
            &root_path,
            &mut visited,
            &mut manifest,
        );
    }
    for label in OPTIONAL_SUBTABLES {
        if let Some(table) = subtable(&main, &root_path, label) {
            digest_table_recursive(
                &table,
                label.to_string(),
                &root_path,
                &mut visited,
                &mut manifest,
            );
        }
    }

    let mut global = StableDigest::default();
    for line in &manifest {
        global.write_string(line);
    }
    let mut out = String::new();
    for line in &manifest {
        out.push_str(line);
        out.push('\n');
    }
    out.push_str(&format!("GLOBAL {}\n", global.hex()));
    out
}

/// The table a manifest label names: `"MAIN"` or a subtable such as
/// `"SOURCE"`, or `None` when the MeasurementSet has no such subtable.
///
/// # Panics
///
/// A table that cannot be opened.
pub fn labelled_table(ms_path: &Path, label: &str) -> Option<Table> {
    let root_path = normalize_existing_path(ms_path);
    let main = open_table(&root_path);
    if label == "MAIN" {
        return Some(main);
    }
    subtable(&main, &root_path, label)
}

/// The digest of every row of `table`, a table of the MeasurementSet at
/// `ms_path`, as the C++ oracle's MAIN-row benchmarks return it.
pub fn table_rows_digest(table: &Table, ms_path: &Path) -> String {
    let root_path = normalize_existing_path(ms_path);
    let mut discovered_refs = BTreeSet::new();
    digest_table_rows(table, &root_path, &mut discovered_refs).hex()
}

/// One row's digest, as [`MeasurementSetOracle::table_row_digest`] returns
/// it, for a table of the MeasurementSet at `ms_path`.
///
/// [`MeasurementSetOracle::table_row_digest`]: crate::ms_interop::MeasurementSetOracle::table_row_digest
pub fn table_row_digest(table: &Table, row_index: usize, ms_path: &Path) -> String {
    let root_path = normalize_existing_path(ms_path);
    let mut digest = StableDigest::default();
    let mut discovered_refs = BTreeSet::new();
    write_row_digest_for_table(
        &mut digest,
        table,
        row_index,
        &root_path,
        &mut discovered_refs,
    );
    digest.hex()
}

/// One row's per-field manifest, as
/// [`MeasurementSetOracle::table_row_field_manifest`] returns it: a line per
/// column with its type, shape and digest.
///
/// [`MeasurementSetOracle::table_row_field_manifest`]: crate::ms_interop::MeasurementSetOracle::table_row_field_manifest
pub fn table_row_field_manifest(table: &Table, row_index: usize, ms_path: &Path) -> String {
    let root_path = normalize_existing_path(ms_path);
    let owner_path = normalize_existing_path(table.path().expect("disk-backed table path"));
    let row = table.row_accessor().row(row_index).expect("row present");
    let mut out = String::new();
    for name in sorted_row_columns(table, row) {
        let defined = table
            .is_cell_defined(row_index, &name)
            .expect("column exists for row manifest");
        let synthesized;
        let value = match row.get(&name) {
            Some(value) => value,
            None => {
                synthesized = synthesized_value(table, &name);
                &synthesized
            }
        };
        let mut digest = StableDigest::default();
        let mut discovered_refs = BTreeSet::new();
        digest.write_string(&name);
        digest.write_string(value_type_tag(value));
        digest.write_bool(defined);
        out.push_str(&name);
        out.push('\t');
        out.push_str(value_type_tag(value));
        if defined {
            digest_value(
                &mut digest,
                value,
                &root_path,
                &mut discovered_refs,
                &owner_path,
            );
        } else {
            let ndim = undefined_cell_ndim(table, &name);
            digest.write_i64(ndim);
            out.push('\t');
            out.push_str(&format!("undefined(ndim={ndim})"));
        }
        if defined && let Value::Array(array) = value {
            let shape = array
                .shape()
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>();
            out.push_str(&format!("\t[{}]", shape.join(",")));
        }
        out.push('\t');
        out.push_str(&digest.hex());
        out.push('\n');
    }
    out
}

#[derive(Debug, Clone, Copy)]
struct StableDigest {
    a: u64,
    b: u64,
}

impl Default for StableDigest {
    fn default() -> Self {
        Self {
            a: 1_469_598_103_934_665_603,
            b: 1_099_511_628_211,
        }
    }
}

impl StableDigest {
    fn write_bytes(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.a ^= u64::from(byte);
            self.a = self.a.wrapping_mul(1_099_511_628_211);
            self.b ^= u64::from(byte);
            self.b = self.b.wrapping_mul(14_029_467_366_897_019_727);
        }
    }

    fn write_u8(&mut self, value: u8) {
        self.write_bytes(&[value]);
    }

    fn write_bool(&mut self, value: bool) {
        self.write_u8(u8::from(value));
    }

    fn write_u32(&mut self, value: u32) {
        self.write_bytes(&value.to_le_bytes());
    }

    fn write_u64(&mut self, value: u64) {
        self.write_bytes(&value.to_le_bytes());
    }

    fn write_i64(&mut self, value: i64) {
        self.write_bytes(&value.to_le_bytes());
    }

    fn write_string(&mut self, value: &str) {
        self.write_u64(value.len() as u64);
        self.write_bytes(value.as_bytes());
    }

    fn hex(self) -> String {
        format!("{:016x}{:016x}", self.a, self.b)
    }
}

fn open_table(path: &Path) -> Table {
    Table::open(TableOptions::new(path))
        .unwrap_or_else(|error| panic!("open {}: {error}", path.display()))
}

/// The subtable the main table's `label` keyword names, if it has one.
fn subtable(main: &Table, root_path: &Path, label: &str) -> Option<Table> {
    let Some(Value::TableRef(reference)) = main.keywords().get(label) else {
        return None;
    };
    Some(open_table(&resolve_table_ref(root_path, reference)))
}

fn normalize_existing_path(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn relative_label(root: &Path, path: &Path) -> String {
    if let Ok(rel) = path.strip_prefix(root) {
        return rel.to_string_lossy().replace('\\', "/");
    }
    path.to_string_lossy().replace('\\', "/")
}

fn primitive_tag(pt: PrimitiveType) -> &'static str {
    match pt {
        PrimitiveType::Bool => "Bool",
        PrimitiveType::UInt8 => "UInt8",
        PrimitiveType::UInt16 => "UInt16",
        PrimitiveType::UInt32 => "UInt32",
        PrimitiveType::Int16 => "Int16",
        PrimitiveType::Int32 => "Int32",
        PrimitiveType::Int64 => "Int64",
        PrimitiveType::Float32 => "Float32",
        PrimitiveType::Float64 => "Float64",
        PrimitiveType::Complex32 => "Complex32",
        PrimitiveType::Complex64 => "Complex64",
        PrimitiveType::String => "String",
    }
}

fn schema_true_type_tag(col: &casa_tables::ColumnSchema) -> String {
    match col.column_type() {
        ColumnType::Scalar => primitive_tag(col.data_type().expect("scalar primitive")).to_string(),
        ColumnType::Array(_) => {
            format!(
                "Array{}",
                primitive_tag(col.data_type().expect("array primitive"))
            )
        }
        ColumnType::Record => "Record".to_string(),
    }
}

fn schema_options_bits(col: &casa_tables::ColumnSchema) -> u32 {
    let opts = col.options();
    let mut bits = 0_u32;
    if opts.direct {
        bits |= 1;
    }
    if opts.undefined {
        bits |= 2;
    }
    if matches!(
        col.column_type(),
        ColumnType::Array(casa_tables::ArrayShapeContract::Fixed { .. })
    ) {
        bits |= 4;
    }
    bits
}

fn schema_ndim_and_shape(col: &casa_tables::ColumnSchema) -> (i64, Vec<usize>) {
    match col.column_type() {
        ColumnType::Scalar | ColumnType::Record => (0, Vec::new()),
        ColumnType::Array(casa_tables::ArrayShapeContract::Fixed { shape }) => {
            (shape.len() as i64, shape.clone())
        }
        ColumnType::Array(casa_tables::ArrayShapeContract::Variable { ndim }) => {
            (ndim.map_or(-1, |n| n as i64), Vec::new())
        }
    }
}

fn write_schema_column(digest: &mut StableDigest, col: &casa_tables::ColumnSchema) {
    digest.write_string(&schema_true_type_tag(col));
    digest.write_u32(schema_options_bits(col));
    let (ndim, shape) = schema_ndim_and_shape(col);
    digest.write_i64(ndim);
    digest.write_u64(shape.len() as u64);
    for dim in shape {
        digest.write_u64(dim as u64);
    }
}

fn write_unknown_schema_column(digest: &mut StableDigest) {
    digest.write_string("Unknown");
    digest.write_u32(0);
    digest.write_i64(-1);
    digest.write_u64(0);
}

fn digest_record(
    digest: &mut StableDigest,
    record: &RecordValue,
    root_path: &Path,
    discovered_refs: &mut BTreeSet<PathBuf>,
    owner_path: &Path,
) {
    let mut fields: Vec<_> = record.fields().iter().collect();
    fields.sort_by(|a, b| a.name.cmp(&b.name));
    digest.write_u64(fields.len() as u64);
    for field in fields {
        digest.write_string(&field.name);
        digest.write_string(value_type_tag(&field.value));
        digest_value(digest, &field.value, root_path, discovered_refs, owner_path);
    }
}

fn value_type_tag(value: &Value) -> &'static str {
    match value {
        Value::Scalar(ScalarValue::Bool(_)) => "Bool",
        Value::Scalar(ScalarValue::UInt8(_)) => "UInt8",
        Value::Scalar(ScalarValue::UInt16(_)) => "UInt16",
        Value::Scalar(ScalarValue::UInt32(_)) => "UInt32",
        Value::Scalar(ScalarValue::Int16(_)) => "Int16",
        Value::Scalar(ScalarValue::Int32(_)) => "Int32",
        Value::Scalar(ScalarValue::Int64(_)) => "Int64",
        Value::Scalar(ScalarValue::Float32(_)) => "Float32",
        Value::Scalar(ScalarValue::Float64(_)) => "Float64",
        Value::Scalar(ScalarValue::Complex32(_)) => "Complex32",
        Value::Scalar(ScalarValue::Complex64(_)) => "Complex64",
        Value::Scalar(ScalarValue::String(_)) => "String",
        Value::Array(ArrayValue::Bool(_)) => "ArrayBool",
        Value::Array(ArrayValue::UInt8(_)) => "ArrayUInt8",
        Value::Array(ArrayValue::UInt16(_)) => "ArrayUInt16",
        Value::Array(ArrayValue::UInt32(_)) => "ArrayUInt32",
        Value::Array(ArrayValue::Int16(_)) => "ArrayInt16",
        Value::Array(ArrayValue::Int32(_)) => "ArrayInt32",
        Value::Array(ArrayValue::Int64(_)) => "ArrayInt64",
        Value::Array(ArrayValue::Float32(_)) => "ArrayFloat32",
        Value::Array(ArrayValue::Float64(_)) => "ArrayFloat64",
        Value::Array(ArrayValue::Complex32(_)) => "ArrayComplex32",
        Value::Array(ArrayValue::Complex64(_)) => "ArrayComplex64",
        Value::Array(ArrayValue::String(_)) => "ArrayString",
        Value::Record(_) => "Record",
        Value::TableRef(_) => "TableRef",
    }
}

/// Digest an array's shape and then its elements in Fortran order.
fn digest_array_with<T>(
    digest: &mut StableDigest,
    shape: &[usize],
    get: impl Fn(&[usize]) -> T,
    mut emit: impl FnMut(&mut StableDigest, T),
) {
    digest.write_u64(shape.len() as u64);
    for &dim in shape {
        digest.write_u64(dim as u64);
    }
    let nelem = if shape.is_empty() {
        1
    } else {
        shape.iter().product()
    };
    if nelem == 0 {
        return;
    }
    let ndim = shape.len();
    let mut index = vec![0_usize; ndim];
    loop {
        emit(digest, get(&index));
        let mut axis = 0;
        while axis < ndim {
            index[axis] += 1;
            if index[axis] < shape[axis] {
                break;
            }
            index[axis] = 0;
            axis += 1;
        }
        if axis == ndim {
            break;
        }
    }
}

fn digest_array(digest: &mut StableDigest, value: &ArrayValue) {
    use ndarray::IxDyn;
    match value {
        ArrayValue::Bool(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[IxDyn(idx)],
            StableDigest::write_bool,
        ),
        ArrayValue::UInt8(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[IxDyn(idx)],
            StableDigest::write_u8,
        ),
        ArrayValue::UInt16(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[IxDyn(idx)],
            |d, v| d.write_bytes(&v.to_le_bytes()),
        ),
        ArrayValue::UInt32(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[IxDyn(idx)],
            StableDigest::write_u32,
        ),
        ArrayValue::Int16(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[IxDyn(idx)],
            |d, v| d.write_bytes(&v.to_le_bytes()),
        ),
        ArrayValue::Int32(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[IxDyn(idx)],
            |d, v| d.write_bytes(&v.to_le_bytes()),
        ),
        ArrayValue::Int64(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[IxDyn(idx)],
            StableDigest::write_i64,
        ),
        ArrayValue::Float32(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[IxDyn(idx)],
            |d, v| d.write_u32(v.to_bits()),
        ),
        ArrayValue::Float64(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[IxDyn(idx)],
            |d, v| d.write_u64(v.to_bits()),
        ),
        ArrayValue::Complex32(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[IxDyn(idx)],
            |d, v| {
                d.write_u32(v.re.to_bits());
                d.write_u32(v.im.to_bits());
            },
        ),
        ArrayValue::Complex64(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[IxDyn(idx)],
            |d, v| {
                d.write_u64(v.re.to_bits());
                d.write_u64(v.im.to_bits());
            },
        ),
        ArrayValue::String(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[IxDyn(idx)].clone(),
            |d, v| d.write_string(&v),
        ),
    }
}

fn resolve_table_ref(owner_path: &Path, rel: &str) -> PathBuf {
    normalize_existing_path(&casa_tables::add_directory(rel, owner_path))
}

fn digest_value(
    digest: &mut StableDigest,
    value: &Value,
    root_path: &Path,
    discovered_refs: &mut BTreeSet<PathBuf>,
    owner_path: &Path,
) {
    match value {
        Value::Scalar(ScalarValue::Bool(v)) => digest.write_bool(*v),
        Value::Scalar(ScalarValue::UInt8(v)) => digest.write_u8(*v),
        Value::Scalar(ScalarValue::UInt16(v)) => digest.write_bytes(&v.to_le_bytes()),
        Value::Scalar(ScalarValue::UInt32(v)) => digest.write_u32(*v),
        Value::Scalar(ScalarValue::Int16(v)) => digest.write_bytes(&v.to_le_bytes()),
        Value::Scalar(ScalarValue::Int32(v)) => digest.write_bytes(&v.to_le_bytes()),
        Value::Scalar(ScalarValue::Int64(v)) => digest.write_i64(*v),
        Value::Scalar(ScalarValue::Float32(v)) => digest.write_u32(v.to_bits()),
        Value::Scalar(ScalarValue::Float64(v)) => digest.write_u64(v.to_bits()),
        Value::Scalar(ScalarValue::Complex32(v)) => {
            digest.write_u32(v.re.to_bits());
            digest.write_u32(v.im.to_bits());
        }
        Value::Scalar(ScalarValue::Complex64(v)) => {
            digest.write_u64(v.re.to_bits());
            digest.write_u64(v.im.to_bits());
        }
        Value::Scalar(ScalarValue::String(v)) => digest.write_string(v),
        Value::Array(arr) => digest_array(digest, arr),
        Value::Record(record) => {
            digest_record(digest, record, root_path, discovered_refs, owner_path);
        }
        Value::TableRef(path) => {
            let resolved = resolve_table_ref(owner_path, path);
            discovered_refs.insert(resolved.clone());
            digest.write_string(&relative_label(root_path, &resolved));
        }
    }
}

/// The table's column names, from its schema or else from its first row.
fn column_names(table: &Table) -> Vec<String> {
    if let Some(schema) = table.schema() {
        return schema
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect();
    }
    table
        .row_accessor()
        .row(0)
        .map(|row| {
            row.fields()
                .iter()
                .map(|field| field.name.clone())
                .collect()
        })
        .unwrap_or_default()
}

/// A row's column names, sorted.
fn sorted_row_columns(table: &Table, row: &RecordValue) -> Vec<String> {
    let mut names: Vec<String> = match table.schema() {
        Some(schema) => schema
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect(),
        None => row
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect(),
    };
    names.sort();
    names
}

/// The value of a field a row omits: the empty record of a record column,
/// or, for an undefined array cell, an empty array of the column's type,
/// which gives the manifest its type tag (its elements are not digested).
fn synthesized_value(table: &Table, name: &str) -> Value {
    let schema = table
        .schema()
        .unwrap_or_else(|| panic!("missing row field {name} without schema"));
    let column = schema
        .column(name)
        .unwrap_or_else(|| panic!("missing row field {name} not in the schema"));
    match (column.column_type(), column.data_type()) {
        (ColumnType::Record, _) => Value::Record(RecordValue::default()),
        (ColumnType::Array(_), Some(data_type)) => Value::Array(empty_array(data_type)),
        _ => panic!("missing value for defined row field {name}"),
    }
}

/// An empty array of `data_type`.
fn empty_array(data_type: PrimitiveType) -> ArrayValue {
    match data_type {
        PrimitiveType::Bool => ArrayValue::from_bool_vec(Vec::new()),
        PrimitiveType::UInt8 => ArrayValue::from_u8_vec(Vec::new()),
        PrimitiveType::UInt16 => ArrayValue::from_u16_vec(Vec::new()),
        PrimitiveType::UInt32 => ArrayValue::from_u32_vec(Vec::new()),
        PrimitiveType::Int16 => ArrayValue::from_i16_vec(Vec::new()),
        PrimitiveType::Int32 => ArrayValue::from_i32_vec(Vec::new()),
        PrimitiveType::Int64 => ArrayValue::from_i64_vec(Vec::new()),
        PrimitiveType::Float32 => ArrayValue::from_f32_vec(Vec::new()),
        PrimitiveType::Float64 => ArrayValue::from_f64_vec(Vec::new()),
        PrimitiveType::Complex32 => ArrayValue::from_complex32_vec(Vec::new()),
        PrimitiveType::Complex64 => ArrayValue::from_complex64_vec(Vec::new()),
        PrimitiveType::String => ArrayValue::from_string_vec(Vec::new()),
    }
}

fn undefined_cell_ndim(table: &Table, name: &str) -> i64 {
    table
        .schema()
        .and_then(|schema| schema.column(name))
        .map_or(-1, |schema| schema_ndim_and_shape(schema).0)
        .max(0)
}

fn digest_table_info(table: &Table) -> StableDigest {
    let mut digest = StableDigest::default();
    digest.write_string(&table.info().table_type);
    digest.write_string(&table.info().sub_type);
    digest
}

fn digest_table_schema(table: &Table) -> StableDigest {
    let mut digest = StableDigest::default();
    if let Some(schema) = table.schema() {
        digest.write_u64(schema.columns().len() as u64);
        for col in schema.columns() {
            digest.write_string(col.name());
            write_schema_column(&mut digest, col);
        }
    } else {
        let names = column_names(table);
        digest.write_u64(names.len() as u64);
        for name in names {
            digest.write_string(&name);
            write_unknown_schema_column(&mut digest);
        }
    }
    digest
}

fn append_table_schema_column_lines(manifest: &mut Vec<String>, table: &Table, label: &str) {
    for name in column_names(table) {
        let mut digest = StableDigest::default();
        digest.write_string(&name);
        match table.schema().and_then(|s| s.column(&name)) {
            Some(schema) => write_schema_column(&mut digest, schema),
            None => write_unknown_schema_column(&mut digest),
        }
        manifest.push(format!("{label}:SCHEMACOL:{name} {}", digest.hex()));
    }
}

fn digest_table_column_keywords(
    table: &Table,
    root_path: &Path,
    discovered_refs: &mut BTreeSet<PathBuf>,
) -> StableDigest {
    let mut digest = StableDigest::default();
    let owner_path = normalize_existing_path(table.path().expect("disk-backed table path"));
    let names = column_names(table);
    digest.write_u64(names.len() as u64);
    for name in names {
        digest.write_string(&name);
        let keywords = table.column_keywords(&name).cloned().unwrap_or_default();
        digest_record(
            &mut digest,
            &keywords,
            root_path,
            discovered_refs,
            &owner_path,
        );
    }
    digest
}

fn digest_table_keywords(
    table: &Table,
    root_path: &Path,
    discovered_refs: &mut BTreeSet<PathBuf>,
) -> StableDigest {
    let mut digest = StableDigest::default();
    let owner_path = normalize_existing_path(table.path().expect("disk-backed table path"));
    digest_record(
        &mut digest,
        table.keywords(),
        root_path,
        discovered_refs,
        &owner_path,
    );
    digest
}

fn write_row_digest_for_table(
    digest: &mut StableDigest,
    table: &Table,
    row_index: usize,
    root_path: &Path,
    discovered_refs: &mut BTreeSet<PathBuf>,
) {
    let owner_path = normalize_existing_path(table.path().expect("disk-backed table path"));
    let row = table.row_accessor().row(row_index).expect("row present");
    let names = sorted_row_columns(table, row);
    digest.write_u64(names.len() as u64);
    for name in names {
        let defined = table
            .is_cell_defined(row_index, &name)
            .expect("column exists for row digest");
        let synthesized;
        let value = match row.get(&name) {
            Some(value) => value,
            None => {
                synthesized = synthesized_value(table, &name);
                &synthesized
            }
        };
        digest.write_string(&name);
        digest.write_string(value_type_tag(value));
        digest.write_bool(defined);
        if defined {
            digest_value(digest, value, root_path, discovered_refs, &owner_path);
        } else {
            digest.write_i64(undefined_cell_ndim(table, &name));
        }
    }
}

fn digest_table_rows(
    table: &Table,
    root_path: &Path,
    discovered_refs: &mut BTreeSet<PathBuf>,
) -> StableDigest {
    let mut digest = StableDigest::default();
    digest.write_u64(table.row_count() as u64);
    for row_index in 0..table.row_count() {
        write_row_digest_for_table(&mut digest, table, row_index, root_path, discovered_refs);
    }
    digest
}

fn digest_table_recursive(
    table: &Table,
    label: String,
    root_path: &Path,
    visited: &mut BTreeSet<PathBuf>,
    manifest: &mut Vec<String>,
) {
    let table_path = normalize_existing_path(table.path().expect("disk-backed table path"));
    if !visited.insert(table_path) {
        return;
    }

    let mut discovered_refs = BTreeSet::new();
    manifest.push(format!("{label}:INFO {}", digest_table_info(table).hex()));
    manifest.push(format!(
        "{label}:SCHEMA {}",
        digest_table_schema(table).hex()
    ));
    append_table_schema_column_lines(manifest, table, &label);
    manifest.push(format!(
        "{label}:COLKW {}",
        digest_table_column_keywords(table, root_path, &mut discovered_refs).hex()
    ));
    manifest.push(format!(
        "{label}:TABLEKW {}",
        digest_table_keywords(table, root_path, &mut discovered_refs).hex()
    ));
    manifest.push(format!(
        "{label}:ROWS {}",
        digest_table_rows(table, root_path, &mut discovered_refs).hex()
    ));

    for ref_path in discovered_refs {
        if visited.contains(&ref_path) {
            continue;
        }
        if !ref_path.exists() {
            let ref_label = format!("EXTRA_MISSING:{}", relative_label(root_path, &ref_path));
            manifest.push(format!("{ref_label} MISSING"));
            continue;
        }
        let ref_table = open_table(&ref_path);
        let ref_label = format!("EXTRA:{}", relative_label(root_path, &ref_path));
        digest_table_recursive(&ref_table, ref_label, root_path, visited, manifest);
    }
}
