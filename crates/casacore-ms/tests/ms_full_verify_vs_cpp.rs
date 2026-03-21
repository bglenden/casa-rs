// SPDX-License-Identifier: LGPL-3.0-or-later
//! Whole-MeasurementSet Rust↔C++ verification.
//!
//! This test compares a stable digest manifest produced from the Rust
//! [`MeasurementSet`] API with one produced by C++ casacore. The manifest
//! covers the main table, standard MS subtables, table/column keywords, and
//! all row values. Any additional linked tables reachable through `TpTable`
//! keyword references are also included.

mod common;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use casacore_ms::SubTable;
use casacore_ms::builder::MeasurementSetBuilder;
use casacore_ms::ms::MeasurementSet;
use casacore_tables::{ColumnType, Table};
use casacore_test_support::cpp_backend_available;
use casacore_test_support::ms_interop::{
    cpp_ms_bench_main_rows, cpp_ms_bench_open_main_rows, cpp_ms_digest_manifest,
    cpp_ms_table_row_digest, cpp_ms_table_row_field_manifest,
};
use casacore_types::{ArrayValue, PrimitiveType, RecordValue, ScalarValue, Value};

use common::{populate_main_rows, populate_subtables};

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
        self.write_u8(if value { 1 } else { 0 });
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

fn schema_true_type_tag(col: &casacore_tables::ColumnSchema) -> String {
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

fn schema_options_bits(col: &casacore_tables::ColumnSchema) -> u32 {
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
        ColumnType::Array(casacore_tables::ArrayShapeContract::Fixed { .. })
    ) {
        bits |= 4;
    }
    bits
}

fn schema_ndim_and_shape(col: &casacore_tables::ColumnSchema) -> (i64, Vec<usize>) {
    match col.column_type() {
        ColumnType::Scalar | ColumnType::Record => (0, Vec::new()),
        ColumnType::Array(casacore_tables::ArrayShapeContract::Fixed { shape }) => {
            (shape.len() as i64, shape.clone())
        }
        ColumnType::Array(casacore_tables::ArrayShapeContract::Variable { ndim }) => {
            (ndim.map(|n| n as i64).unwrap_or(-1), Vec::new())
        }
    }
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
    match value {
        ArrayValue::Bool(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[ndarray::IxDyn(idx)],
            |d, v| d.write_bool(v),
        ),
        ArrayValue::UInt8(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[ndarray::IxDyn(idx)],
            |d, v| d.write_u8(v),
        ),
        ArrayValue::UInt16(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[ndarray::IxDyn(idx)],
            |d, v| d.write_bytes(&v.to_le_bytes()),
        ),
        ArrayValue::UInt32(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[ndarray::IxDyn(idx)],
            |d, v| d.write_u32(v),
        ),
        ArrayValue::Int16(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[ndarray::IxDyn(idx)],
            |d, v| d.write_bytes(&v.to_le_bytes()),
        ),
        ArrayValue::Int32(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[ndarray::IxDyn(idx)],
            |d, v| d.write_bytes(&v.to_le_bytes()),
        ),
        ArrayValue::Int64(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[ndarray::IxDyn(idx)],
            |d, v| d.write_i64(v),
        ),
        ArrayValue::Float32(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[ndarray::IxDyn(idx)],
            |d, v| d.write_u32(v.to_bits()),
        ),
        ArrayValue::Float64(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[ndarray::IxDyn(idx)],
            |d, v| d.write_u64(v.to_bits()),
        ),
        ArrayValue::Complex32(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[ndarray::IxDyn(idx)],
            |d, v| {
                d.write_u32(v.re.to_bits());
                d.write_u32(v.im.to_bits());
            },
        ),
        ArrayValue::Complex64(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[ndarray::IxDyn(idx)],
            |d, v| {
                d.write_u64(v.re.to_bits());
                d.write_u64(v.im.to_bits());
            },
        ),
        ArrayValue::String(arr) => digest_array_with(
            digest,
            arr.shape(),
            |idx| arr[ndarray::IxDyn(idx)].clone(),
            |d, v| d.write_string(&v),
        ),
    }
}

fn resolve_table_ref(owner_path: &Path, rel: &str) -> PathBuf {
    normalize_existing_path(&owner_path.join(rel))
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
            digest_record(digest, record, root_path, discovered_refs, owner_path)
        }
        Value::TableRef(path) => {
            let resolved = resolve_table_ref(owner_path, path);
            discovered_refs.insert(resolved.clone());
            digest.write_string(&relative_label(root_path, &resolved));
        }
    }
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
            digest.write_string(&schema_true_type_tag(col));
            digest.write_u32(schema_options_bits(col));
            let (ndim, shape) = schema_ndim_and_shape(col);
            digest.write_i64(ndim);
            digest.write_u64(shape.len() as u64);
            for dim in shape {
                digest.write_u64(dim as u64);
            }
        }
    } else {
        let names: Vec<String> = table
            .row(0)
            .map(|row| {
                row.fields()
                    .iter()
                    .map(|field| field.name.clone())
                    .collect()
            })
            .unwrap_or_default();
        digest.write_u64(names.len() as u64);
        for name in names {
            digest.write_string(&name);
            digest.write_string("Unknown");
            digest.write_u32(0);
            digest.write_i64(-1);
            digest.write_u64(0);
        }
    }
    digest
}

fn append_table_schema_column_lines(manifest: &mut Vec<String>, table: &Table, label: &str) {
    let column_names: Vec<String> = if let Some(schema) = table.schema() {
        schema
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect()
    } else {
        table
            .row(0)
            .map(|row| {
                row.fields()
                    .iter()
                    .map(|field| field.name.clone())
                    .collect()
            })
            .unwrap_or_default()
    };

    for name in column_names {
        let mut digest = StableDigest::default();
        digest.write_string(&name);
        if let Some(schema) = table.schema().and_then(|s| s.column(&name)) {
            digest.write_string(&schema_true_type_tag(schema));
            digest.write_u32(schema_options_bits(schema));
            let (ndim, shape) = schema_ndim_and_shape(schema);
            digest.write_i64(ndim);
            digest.write_u64(shape.len() as u64);
            for dim in shape {
                digest.write_u64(dim as u64);
            }
        } else {
            digest.write_string("Unknown");
            digest.write_u32(0);
            digest.write_i64(-1);
            digest.write_u64(0);
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
    let column_names: Vec<String> = if let Some(schema) = table.schema() {
        schema
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect()
    } else {
        table
            .row(0)
            .map(|row| {
                row.fields()
                    .iter()
                    .map(|field| field.name.clone())
                    .collect()
            })
            .unwrap_or_default()
    };
    digest.write_u64(column_names.len() as u64);
    for name in column_names {
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

fn digest_table_keywords_digest(
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
    let row = table.row(row_index).expect("row present");
    let mut column_names: Vec<String> = if let Some(schema) = table.schema() {
        schema
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect()
    } else {
        row.fields()
            .iter()
            .map(|field| field.name.clone())
            .collect()
    };
    column_names.sort();
    digest.write_u64(column_names.len() as u64);
    for name in column_names {
        let defined = table
            .is_cell_defined(row_index, &name)
            .expect("column exists for row digest");
        let field = row.get(&name);
        let synthesized;
        let value = if let Some(value) = field {
            value
        } else if let Some(schema) = table.schema() {
            if matches!(
                schema
                    .column(&name)
                    .map(casacore_tables::ColumnSchema::column_type),
                Some(ColumnType::Record)
            ) {
                synthesized = Value::Record(RecordValue::default());
                &synthesized
            } else {
                panic!("missing value for defined row field {name}");
            }
        } else {
            panic!("missing row field {name} without schema");
        };
        digest.write_string(&name);
        digest.write_string(value_type_tag(value));
        digest.write_bool(defined);
        if defined {
            digest_value(digest, value, root_path, discovered_refs, &owner_path);
        } else {
            let ndim = table
                .schema()
                .and_then(|schema| schema.column(&name))
                .map(|schema| schema_ndim_and_shape(schema).0)
                .unwrap_or(-1);
            digest.write_i64(ndim.max(0));
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

fn row_digest_for_table(table: &Table, row_index: usize, root_path: &Path) -> StableDigest {
    let mut digest = StableDigest::default();
    let mut discovered_refs = BTreeSet::new();
    write_row_digest_for_table(
        &mut digest,
        table,
        row_index,
        root_path,
        &mut discovered_refs,
    );
    digest
}

fn row_field_manifest_for_table(table: &Table, row_index: usize, root_path: &Path) -> String {
    let owner_path = normalize_existing_path(table.path().expect("disk-backed table path"));
    let row = table.row(row_index).expect("row present");
    let mut column_names: Vec<String> = if let Some(schema) = table.schema() {
        schema
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect()
    } else {
        row.fields()
            .iter()
            .map(|field| field.name.clone())
            .collect()
    };
    column_names.sort();
    let mut out = String::new();
    for name in column_names {
        let defined = table
            .is_cell_defined(row_index, &name)
            .expect("column exists for row manifest");
        let field = row.get(&name);
        let synthesized;
        let value = if let Some(value) = field {
            value
        } else if let Some(schema) = table.schema() {
            if matches!(
                schema
                    .column(&name)
                    .map(casacore_tables::ColumnSchema::column_type),
                Some(ColumnType::Record)
            ) {
                synthesized = Value::Record(RecordValue::default());
                &synthesized
            } else {
                panic!("missing value for defined row field {name}");
            }
        } else {
            panic!("missing row field {name} without schema");
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
                root_path,
                &mut discovered_refs,
                &owner_path,
            );
        } else {
            let ndim = table
                .schema()
                .and_then(|schema| schema.column(&name))
                .map(|schema| schema_ndim_and_shape(schema).0)
                .unwrap_or(-1);
            digest.write_i64(ndim.max(0));
            out.push('\t');
            out.push_str(&format!("undefined(ndim={})", ndim.max(0)));
        }
        if defined && let Value::Array(arr) = value {
            out.push('\t');
            out.push('[');
            let shape = match arr {
                ArrayValue::Bool(a) => a.shape(),
                ArrayValue::UInt8(a) => a.shape(),
                ArrayValue::UInt16(a) => a.shape(),
                ArrayValue::UInt32(a) => a.shape(),
                ArrayValue::Int16(a) => a.shape(),
                ArrayValue::Int32(a) => a.shape(),
                ArrayValue::Int64(a) => a.shape(),
                ArrayValue::Float32(a) => a.shape(),
                ArrayValue::Float64(a) => a.shape(),
                ArrayValue::Complex32(a) => a.shape(),
                ArrayValue::Complex64(a) => a.shape(),
                ArrayValue::String(a) => a.shape(),
            };
            for (i, dim) in shape.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&dim.to_string());
            }
            out.push(']');
        }
        out.push('\t');
        out.push_str(&digest.hex());
        out.push('\n');
    }
    out
}

fn print_table_row_diagnostics(ms: &MeasurementSet, ms_path: &Path, label: &str) {
    let root_path = normalize_existing_path(ms.path().expect("MeasurementSet path"));
    let inspect = |table: &Table| {
        let limit = table.row_count().min(8);
        for row_index in 0..limit {
            let rust_digest = row_digest_for_table(table, row_index, &root_path).hex();
            let cpp_digest = cpp_ms_table_row_digest(ms_path, label, row_index as u64)
                .unwrap_or_else(|err| format!("ERROR:{err}"));
            if rust_digest != cpp_digest {
                eprintln!(
                    "{label} row {row_index} digest mismatch: rust={rust_digest} cpp={cpp_digest}"
                );
                let rust_fields = row_field_manifest_for_table(table, row_index, &root_path);
                let cpp_fields = cpp_ms_table_row_field_manifest(ms_path, label, row_index as u64)
                    .unwrap_or_else(|err| format!("ERROR:{err}\n"));
                let rust_lines: Vec<_> = rust_fields.lines().collect();
                let cpp_lines: Vec<_> = cpp_fields.lines().collect();
                let max_len = rust_lines.len().max(cpp_lines.len());
                let mut shown = 0;
                for i in 0..max_len {
                    let rust = rust_lines.get(i).copied().unwrap_or("<missing>");
                    let cpp = cpp_lines.get(i).copied().unwrap_or("<missing>");
                    if rust != cpp {
                        eprintln!("{label} row {row_index} field rust: {rust}");
                        eprintln!("{label} row {row_index} field cpp : {cpp}");
                        shown += 1;
                        if shown >= 20 {
                            break;
                        }
                    }
                }
                break;
            }
        }
    };

    match label {
        "MAIN" => inspect(ms.main_table()),
        "ANTENNA" => inspect(ms.antenna().expect("ANTENNA").table()),
        "DATA_DESCRIPTION" => inspect(ms.data_description().expect("DATA_DESCRIPTION").table()),
        "FEED" => inspect(ms.feed().expect("FEED").table()),
        "FIELD" => inspect(ms.field().expect("FIELD").table()),
        "FLAG_CMD" => inspect(ms.flag_cmd().expect("FLAG_CMD").table()),
        "HISTORY" => inspect(ms.history().expect("HISTORY").table()),
        "OBSERVATION" => inspect(ms.observation().expect("OBSERVATION").table()),
        "POINTING" => inspect(ms.pointing().expect("POINTING").table()),
        "POLARIZATION" => inspect(ms.polarization().expect("POLARIZATION").table()),
        "PROCESSOR" => inspect(ms.processor().expect("PROCESSOR").table()),
        "SPECTRAL_WINDOW" => inspect(ms.spectral_window().expect("SPECTRAL_WINDOW").table()),
        "STATE" => inspect(ms.state().expect("STATE").table()),
        "DOPPLER" => inspect(ms.doppler().expect("DOPPLER").table()),
        "FREQ_OFFSET" => inspect(ms.freq_offset().expect("FREQ_OFFSET").table()),
        "SOURCE" => inspect(ms.source().expect("SOURCE").table()),
        "SYSCAL" => inspect(ms.syscal().expect("SYSCAL").table()),
        "WEATHER" => inspect(ms.weather().expect("WEATHER").table()),
        _ => panic!("unknown MeasurementSet table label: {label}"),
    }
}

fn digest_table_recursive(
    table: &Table,
    label: String,
    root_path: &Path,
    visited: &mut BTreeSet<PathBuf>,
    manifest: &mut Vec<String>,
) {
    let table_path = normalize_existing_path(table.path().expect("disk-backed table path"));
    if !visited.insert(table_path.clone()) {
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
        digest_table_keywords_digest(table, root_path, &mut discovered_refs).hex()
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
        let ref_table = Table::open(casacore_tables::TableOptions::new(&ref_path))
            .expect("open referenced table");
        let ref_label = format!("EXTRA:{}", relative_label(root_path, &ref_path));
        digest_table_recursive(&ref_table, ref_label, root_path, visited, manifest);
    }
}

fn digest_measurement_set_manifest(ms: &MeasurementSet) -> String {
    let root_path = normalize_existing_path(ms.path().expect("MeasurementSet path"));
    let mut visited = BTreeSet::new();
    let mut manifest = Vec::new();

    digest_table_recursive(
        ms.main_table(),
        "MAIN".to_string(),
        &root_path,
        &mut visited,
        &mut manifest,
    );

    let digest_subtable = |table: &Table,
                           label: &str,
                           root_path: &Path,
                           visited: &mut BTreeSet<PathBuf>,
                           manifest: &mut Vec<String>| {
        digest_table_recursive(table, label.to_string(), root_path, visited, manifest);
    };

    digest_subtable(
        ms.antenna().expect("ANTENNA").table(),
        "ANTENNA",
        &root_path,
        &mut visited,
        &mut manifest,
    );
    digest_subtable(
        ms.data_description().expect("DATA_DESCRIPTION").table(),
        "DATA_DESCRIPTION",
        &root_path,
        &mut visited,
        &mut manifest,
    );
    digest_subtable(
        ms.feed().expect("FEED").table(),
        "FEED",
        &root_path,
        &mut visited,
        &mut manifest,
    );
    digest_subtable(
        ms.field().expect("FIELD").table(),
        "FIELD",
        &root_path,
        &mut visited,
        &mut manifest,
    );
    digest_subtable(
        ms.flag_cmd().expect("FLAG_CMD").table(),
        "FLAG_CMD",
        &root_path,
        &mut visited,
        &mut manifest,
    );
    digest_subtable(
        ms.history().expect("HISTORY").table(),
        "HISTORY",
        &root_path,
        &mut visited,
        &mut manifest,
    );
    digest_subtable(
        ms.observation().expect("OBSERVATION").table(),
        "OBSERVATION",
        &root_path,
        &mut visited,
        &mut manifest,
    );
    digest_subtable(
        ms.pointing().expect("POINTING").table(),
        "POINTING",
        &root_path,
        &mut visited,
        &mut manifest,
    );
    digest_subtable(
        ms.polarization().expect("POLARIZATION").table(),
        "POLARIZATION",
        &root_path,
        &mut visited,
        &mut manifest,
    );
    digest_subtable(
        ms.processor().expect("PROCESSOR").table(),
        "PROCESSOR",
        &root_path,
        &mut visited,
        &mut manifest,
    );
    digest_subtable(
        ms.spectral_window().expect("SPECTRAL_WINDOW").table(),
        "SPECTRAL_WINDOW",
        &root_path,
        &mut visited,
        &mut manifest,
    );
    digest_subtable(
        ms.state().expect("STATE").table(),
        "STATE",
        &root_path,
        &mut visited,
        &mut manifest,
    );

    if let Ok(table) = ms.doppler() {
        digest_subtable(
            table.table(),
            "DOPPLER",
            &root_path,
            &mut visited,
            &mut manifest,
        );
    }
    if let Ok(table) = ms.freq_offset() {
        digest_subtable(
            table.table(),
            "FREQ_OFFSET",
            &root_path,
            &mut visited,
            &mut manifest,
        );
    }
    if let Ok(table) = ms.source() {
        digest_subtable(
            table.table(),
            "SOURCE",
            &root_path,
            &mut visited,
            &mut manifest,
        );
    }
    if let Ok(table) = ms.syscal() {
        digest_subtable(
            table.table(),
            "SYSCAL",
            &root_path,
            &mut visited,
            &mut manifest,
        );
    }
    if let Ok(table) = ms.weather() {
        digest_subtable(
            table.table(),
            "WEATHER",
            &root_path,
            &mut visited,
            &mut manifest,
        );
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

#[test]
fn ms_full_manifest_matches_cpp_for_basic_fixture() {
    if !cpp_backend_available() {
        eprintln!(
            "skipping ms_full_manifest_matches_cpp_for_basic_fixture: C++ casacore not available"
        );
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let ms_path = dir.path().join("full_manifest_basic.ms");

    let builder = MeasurementSetBuilder::new().with_main_column("DATA");
    let mut ms = MeasurementSet::create(&ms_path, builder).unwrap();
    populate_subtables(&mut ms);
    populate_main_rows(&mut ms, 6);
    ms.save().unwrap();

    let ms = MeasurementSet::open(&ms_path).unwrap();
    let rust_manifest = digest_measurement_set_manifest(&ms);
    let cpp_manifest = cpp_ms_digest_manifest(&ms_path).unwrap();
    assert_eq!(rust_manifest, cpp_manifest);
}

/// Run a whole-MS Rust↔C++ parity check against an external MeasurementSet.
///
/// Set `CASA_RS_VERIFY_MS_PATH` to the MS root directory. This is intended
/// for downloaded benchmark datasets kept outside the repo, for example:
///
/// `CASA_RS_VERIFY_MS_PATH=/path/to/ref_vlass_wtsp_creation.ms cargo test -p casacore-ms --test ms_full_verify_vs_cpp verify_external_ms_matches_cpp -- --nocapture`
#[test]
fn verify_external_ms_matches_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping verify_external_ms_matches_cpp: C++ casacore not available");
        return;
    }

    let Some(path) = std::env::var_os("CASA_RS_VERIFY_MS_PATH") else {
        eprintln!("skipping verify_external_ms_matches_cpp: CASA_RS_VERIFY_MS_PATH not set");
        return;
    };
    let ms_path = PathBuf::from(path);
    if !ms_path.is_dir() {
        eprintln!(
            "skipping verify_external_ms_matches_cpp: path is not a directory: {}",
            ms_path.display()
        );
        return;
    }

    let ms = MeasurementSet::open(&ms_path).expect("open MeasurementSet in Rust");
    let rust_manifest = digest_measurement_set_manifest(&ms);
    let cpp_manifest = cpp_ms_digest_manifest(&ms_path).expect("open MeasurementSet in C++");
    if rust_manifest != cpp_manifest {
        eprintln!("row counts:");
        eprintln!("MAIN {}", ms.row_count());
        eprintln!(
            "DATA_DESCRIPTION {}",
            ms.data_description().unwrap().table().row_count()
        );
        eprintln!(
            "POLARIZATION {}",
            ms.polarization().unwrap().table().row_count()
        );
        eprintln!("PROCESSOR {}", ms.processor().unwrap().table().row_count());
        eprintln!("STATE {}", ms.state().unwrap().table().row_count());
        eprintln!("SOURCE {}", ms.source().unwrap().table().row_count());
        eprintln!("SYSCAL {}", ms.syscal().unwrap().table().row_count());
        eprintln!("WEATHER {}", ms.weather().unwrap().table().row_count());
        eprintln!("HISTORY {}", ms.history().unwrap().table().row_count());
        let rust_lines: Vec<_> = rust_manifest.lines().collect();
        let cpp_lines: Vec<_> = cpp_manifest.lines().collect();
        let max_len = rust_lines.len().max(cpp_lines.len());
        let mut shown = 0;
        eprintln!("first manifest diffs:");
        for i in 0..max_len {
            let rust = rust_lines.get(i).copied().unwrap_or("<missing>");
            let cpp = cpp_lines.get(i).copied().unwrap_or("<missing>");
            if rust != cpp {
                eprintln!("line {} rust: {}", i + 1, rust);
                eprintln!("line {} cpp : {}", i + 1, cpp);
                shown += 1;
                if shown >= 20 {
                    break;
                }
            }
        }
        for label in ["MAIN", "SOURCE", "SYSCAL"] {
            print_table_row_diagnostics(&ms, &ms_path, label);
        }
    }
    assert_eq!(rust_manifest, cpp_manifest);
}

#[test]
#[ignore = "debug helper for inspecting one external MeasurementSet row"]
fn debug_external_ms_row_vs_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping debug_external_ms_row_vs_cpp: C++ casacore not available");
        return;
    }

    let Some(path) = std::env::var_os("CASA_RS_VERIFY_MS_PATH") else {
        eprintln!("skipping debug_external_ms_row_vs_cpp: CASA_RS_VERIFY_MS_PATH not set");
        return;
    };
    let label = std::env::var("CASA_RS_VERIFY_MS_LABEL").unwrap_or_else(|_| "MAIN".to_string());
    let row_index = std::env::var("CASA_RS_VERIFY_MS_ROW")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);

    let ms_path = PathBuf::from(path);
    let ms = MeasurementSet::open(&ms_path).expect("open MeasurementSet in Rust");
    let root_path = normalize_existing_path(ms.path().expect("MeasurementSet path"));
    let print = |table: &Table| {
        eprintln!(
            "rust digest: {}",
            row_digest_for_table(table, row_index, &root_path).hex()
        );
        eprintln!(
            "cpp  digest: {}",
            cpp_ms_table_row_digest(&ms_path, &label, row_index as u64).expect("cpp row digest"),
        );
        eprintln!(
            "rust fields:\n{}",
            row_field_manifest_for_table(table, row_index, &root_path)
        );
        eprintln!(
            "cpp fields:\n{}",
            cpp_ms_table_row_field_manifest(&ms_path, &label, row_index as u64)
                .expect("cpp row field manifest"),
        );
    };
    match label.as_str() {
        "MAIN" => print(ms.main_table()),
        "ANTENNA" => print(ms.antenna().expect("ANTENNA").table()),
        "DATA_DESCRIPTION" => print(ms.data_description().expect("DATA_DESCRIPTION").table()),
        "FEED" => print(ms.feed().expect("FEED").table()),
        "FIELD" => print(ms.field().expect("FIELD").table()),
        "FLAG_CMD" => print(ms.flag_cmd().expect("FLAG_CMD").table()),
        "HISTORY" => print(ms.history().expect("HISTORY").table()),
        "OBSERVATION" => print(ms.observation().expect("OBSERVATION").table()),
        "POINTING" => print(ms.pointing().expect("POINTING").table()),
        "POLARIZATION" => print(ms.polarization().expect("POLARIZATION").table()),
        "PROCESSOR" => print(ms.processor().expect("PROCESSOR").table()),
        "SPECTRAL_WINDOW" => print(ms.spectral_window().expect("SPECTRAL_WINDOW").table()),
        "STATE" => print(ms.state().expect("STATE").table()),
        "DOPPLER" => print(ms.doppler().expect("DOPPLER").table()),
        "FREQ_OFFSET" => print(ms.freq_offset().expect("FREQ_OFFSET").table()),
        "SOURCE" => print(ms.source().expect("SOURCE").table()),
        "SYSCAL" => print(ms.syscal().expect("SYSCAL").table()),
        "WEATHER" => print(ms.weather().expect("WEATHER").table()),
        other => panic!("unknown MeasurementSet table label: {other}"),
    }
}

#[cfg(target_os = "macos")]
fn purge_disk_cache() -> Result<(), String> {
    let purge = Path::new("/usr/sbin/purge");
    if !purge.is_file() {
        return Err(format!("expected macOS purge tool at {}", purge.display()));
    }
    let status = Command::new(purge)
        .status()
        .map_err(|err| format!("run macOS purge: {err}"))?;
    if !status.success() {
        return Err(format!("macOS purge exited with status {status}"));
    }
    Ok(())
}

/// Benchmark reading all rows of the MAIN table from an external MeasurementSet.
///
/// This reuses the same definedness-aware MAIN-row digest that the verifier uses,
/// but only times the MAIN table scan rather than traversing the full MS.
///
/// Set `CASA_RS_VERIFY_MS_PATH` to the external MS root and run, for example:
///
/// `CASA_RS_VERIFY_MS_PATH=/path/to/uid___A002_Xb9dfa4_X4724_target_spw16.ms cargo test --release -p casacore-ms --test ms_full_verify_vs_cpp bench_external_main_rows_vs_cpp -- --ignored --nocapture`
#[test]
#[ignore = "performance helper for external MeasurementSet MAIN-table scans"]
fn bench_external_main_rows_vs_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping bench_external_main_rows_vs_cpp: C++ casacore not available");
        return;
    }

    let Some(path) = std::env::var_os("CASA_RS_VERIFY_MS_PATH") else {
        eprintln!("skipping bench_external_main_rows_vs_cpp: CASA_RS_VERIFY_MS_PATH not set");
        return;
    };
    let ms_path = PathBuf::from(path);
    if !ms_path.is_dir() {
        eprintln!(
            "skipping bench_external_main_rows_vs_cpp: path is not a directory: {}",
            ms_path.display()
        );
        return;
    }

    let cpp = cpp_ms_bench_main_rows(&ms_path).expect("benchmark MAIN rows in C++");

    let ms = MeasurementSet::open(&ms_path).expect("open MeasurementSet in Rust");
    let root_path = normalize_existing_path(ms.path().expect("MeasurementSet path"));
    let mut discovered_refs = BTreeSet::new();
    let t0 = Instant::now();
    let rust_digest = digest_table_rows(ms.main_table(), &root_path, &mut discovered_refs).hex();
    let rust_read_ns = t0.elapsed().as_nanos() as u64;

    assert_eq!(rust_digest, cpp.rows_digest);

    let read_ratio = rust_read_ns as f64 / cpp.read_ns.max(1) as f64;
    eprintln!(
        "MAIN-table full scan ({} rows):\n  C++ {:.1} ms\n  Rust {:.1} ms\n  ratio {:.2}x",
        ms.row_count(),
        cpp.read_ns as f64 / 1e6,
        rust_read_ns as f64 / 1e6,
        read_ratio,
    );
    if read_ratio > 5.0 {
        eprintln!("  warning: Rust MAIN-table scan is {read_ratio:.2}x slower than C++");
    }
}

/// Benchmark opening an external MeasurementSet and then streaming the full MAIN
/// table, without attempting to purge the OS file cache.
///
/// This is still a fresh-process `open + read` measurement, so it captures Rust
/// and C++ open-path costs even when a cold-cache approximation is unavailable.
#[test]
#[ignore = "performance helper for external MeasurementSet open + MAIN scans"]
fn bench_external_open_and_main_rows_vs_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping bench_external_open_and_main_rows_vs_cpp: C++ casacore not available");
        return;
    }

    let Some(path) = std::env::var_os("CASA_RS_VERIFY_MS_PATH") else {
        eprintln!(
            "skipping bench_external_open_and_main_rows_vs_cpp: CASA_RS_VERIFY_MS_PATH not set"
        );
        return;
    };
    let ms_path = PathBuf::from(path);
    if !ms_path.is_dir() {
        eprintln!(
            "skipping bench_external_open_and_main_rows_vs_cpp: path is not a directory: {}",
            ms_path.display()
        );
        return;
    }

    let cpp = cpp_ms_bench_open_main_rows(&ms_path).expect("benchmark MAIN open+scan in C++");

    let t0 = Instant::now();
    let ms = MeasurementSet::open(&ms_path).expect("open MeasurementSet in Rust");
    let root_path = normalize_existing_path(ms.path().expect("MeasurementSet path"));
    let mut discovered_refs = BTreeSet::new();
    let rust_digest = digest_table_rows(ms.main_table(), &root_path, &mut discovered_refs).hex();
    let rust_open_and_read_ns = t0.elapsed().as_nanos() as u64;

    assert_eq!(rust_digest, cpp.rows_digest);

    let total_ratio = rust_open_and_read_ns as f64 / cpp.open_and_read_ns.max(1) as f64;
    eprintln!(
        "MAIN-table open + full scan ({} rows):\n  C++ {:.1} ms\n  Rust {:.1} ms\n  ratio {:.2}x",
        ms.row_count(),
        cpp.open_and_read_ns as f64 / 1e6,
        rust_open_and_read_ns as f64 / 1e6,
        total_ratio,
    );
    if total_ratio > 5.0 {
        eprintln!("  warning: Rust MAIN open+scan is {total_ratio:.2}x slower than C++");
    }
}

/// Benchmark opening an external MeasurementSet and then streaming the full MAIN
/// table under a cold-ish macOS buffer-cache approximation.
///
/// This test is macOS-only because it uses `/usr/sbin/purge` between the C++ and
/// Rust passes. It is still not a true cold-boot measurement, but it is much
/// closer to real `open + read` timing than the warm post-open scan benchmark.
///
/// Set `CASA_RS_VERIFY_MS_PATH` to the external MS root and run, for example:
///
/// `CASA_RS_VERIFY_MS_PATH=/path/to/uid___A002_Xb9dfa4_X4724_target_spw16.ms cargo test --release -p casacore-ms --test ms_full_verify_vs_cpp bench_external_open_and_main_rows_vs_cpp_coldish -- --ignored --nocapture`
#[test]
#[ignore = "performance helper for macOS cold-ish external MeasurementSet open + MAIN scans"]
#[cfg(target_os = "macos")]
fn bench_external_open_and_main_rows_vs_cpp_coldish() {
    if !cpp_backend_available() {
        eprintln!(
            "skipping bench_external_open_and_main_rows_vs_cpp_coldish: C++ casacore not available"
        );
        return;
    }

    let Some(path) = std::env::var_os("CASA_RS_VERIFY_MS_PATH") else {
        eprintln!(
            "skipping bench_external_open_and_main_rows_vs_cpp_coldish: CASA_RS_VERIFY_MS_PATH not set"
        );
        return;
    };
    let ms_path = PathBuf::from(path);
    if !ms_path.is_dir() {
        eprintln!(
            "skipping bench_external_open_and_main_rows_vs_cpp_coldish: path is not a directory: {}",
            ms_path.display()
        );
        return;
    }

    let Err(err) = purge_disk_cache() else {
        let cpp = cpp_ms_bench_open_main_rows(&ms_path).expect("benchmark MAIN open+scan in C++");

        let Err(err) = purge_disk_cache() else {
            let t0 = Instant::now();
            let ms = MeasurementSet::open(&ms_path).expect("open MeasurementSet in Rust");
            let root_path = normalize_existing_path(ms.path().expect("MeasurementSet path"));
            let mut discovered_refs = BTreeSet::new();
            let rust_digest =
                digest_table_rows(ms.main_table(), &root_path, &mut discovered_refs).hex();
            let rust_open_and_read_ns = t0.elapsed().as_nanos() as u64;

            assert_eq!(rust_digest, cpp.rows_digest);

            let total_ratio = rust_open_and_read_ns as f64 / cpp.open_and_read_ns.max(1) as f64;
            eprintln!(
                "MAIN-table cold-ish open + full scan ({} rows):\n  C++ {:.1} ms\n  Rust {:.1} ms\n  ratio {:.2}x",
                ms.row_count(),
                cpp.open_and_read_ns as f64 / 1e6,
                rust_open_and_read_ns as f64 / 1e6,
                total_ratio,
            );
            if total_ratio > 5.0 {
                eprintln!("  warning: Rust MAIN open+scan is {total_ratio:.2}x slower than C++");
            }
            return;
        };
        eprintln!(
            "skipping bench_external_open_and_main_rows_vs_cpp_coldish: second purge failed: {err}"
        );
        return;
    };
    eprintln!(
        "skipping bench_external_open_and_main_rows_vs_cpp_coldish: purge unavailable: {err}"
    );
}
