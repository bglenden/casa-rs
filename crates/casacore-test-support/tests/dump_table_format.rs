// Temporary diagnostic test to compare C++ vs Rust table binary output.
use casacore_tables::{ColumnSchema, Table, TableOptions, TableSchema};
use casacore_test_support::CppTableFixture;
use casacore_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

#[test]
fn dump_scalar_primitives_comparison() {
    let cpp_dir = tempfile::tempdir().unwrap();
    let cpp_path = cpp_dir.path().join("cpp_table");

    // Write C++ table
    casacore_test_support::cpp_table_write(CppTableFixture::ScalarPrimitives, &cpp_path)
        .expect("C++ write");

    // Write Rust table
    let rust_dir = tempfile::tempdir().unwrap();
    let rust_path = rust_dir.path().join("rust_table");

    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("col_bool", PrimitiveType::Bool),
        ColumnSchema::scalar("col_i32", PrimitiveType::Int32),
        ColumnSchema::scalar("col_f64", PrimitiveType::Float64),
        ColumnSchema::scalar("col_str", PrimitiveType::String),
    ])
    .expect("schema");

    let rows = vec![
        RecordValue::new(vec![
            RecordField::new("col_bool", Value::Scalar(ScalarValue::Bool(true))),
            RecordField::new("col_i32", Value::Scalar(ScalarValue::Int32(42))),
            RecordField::new("col_f64", Value::Scalar(ScalarValue::Float64(1.5))),
            RecordField::new(
                "col_str",
                Value::Scalar(ScalarValue::String("hello".to_string())),
            ),
        ]),
        RecordValue::new(vec![
            RecordField::new("col_bool", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("col_i32", Value::Scalar(ScalarValue::Int32(-7))),
            RecordField::new("col_f64", Value::Scalar(ScalarValue::Float64(-99.5))),
            RecordField::new(
                "col_str",
                Value::Scalar(ScalarValue::String("world".to_string())),
            ),
        ]),
        RecordValue::new(vec![
            RecordField::new("col_bool", Value::Scalar(ScalarValue::Bool(true))),
            RecordField::new("col_i32", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("col_f64", Value::Scalar(ScalarValue::Float64(0.0))),
            RecordField::new("col_str", Value::Scalar(ScalarValue::String(String::new()))),
        ]),
    ];

    let mut table = Table::from_rows_with_schema(rows, schema).expect("create table");
    table.keywords_mut().push(RecordField::new(
        "observer",
        Value::Scalar(ScalarValue::String("test-harness".to_string())),
    ));
    table.save(TableOptions::new(&rust_path)).expect("save");

    // Dump table.dat from both
    let cpp_dat = std::fs::read(cpp_path.join("table.dat")).expect("read cpp table.dat");
    let rust_dat = std::fs::read(rust_path.join("table.dat")).expect("read rust table.dat");

    eprintln!("=== C++ table.dat ({} bytes) ===", cpp_dat.len());
    hex_dump(&cpp_dat);
    eprintln!("\n=== Rust table.dat ({} bytes) ===", rust_dat.len());
    hex_dump(&rust_dat);

    // Dump table.f0 from both
    let cpp_f0 = std::fs::read(cpp_path.join("table.f0")).expect("read cpp table.f0");
    let rust_f0 = std::fs::read(rust_path.join("table.f0")).expect("read rust table.f0");

    eprintln!("\n=== C++ table.f0 ({} bytes) ===", cpp_f0.len());
    hex_dump(&cpp_f0);
    eprintln!("\n=== Rust table.f0 ({} bytes) ===", rust_f0.len());
    hex_dump(&rust_f0);

    // List all files in both dirs
    eprintln!("\n=== C++ table files ===");
    for entry in std::fs::read_dir(&cpp_path).unwrap() {
        let entry = entry.unwrap();
        let meta = entry.metadata().unwrap();
        eprintln!(
            "  {} ({} bytes)",
            entry.file_name().to_string_lossy(),
            meta.len()
        );
    }

    eprintln!("\n=== Rust table files ===");
    for entry in std::fs::read_dir(&rust_path).unwrap() {
        let entry = entry.unwrap();
        let meta = entry.metadata().unwrap();
        eprintln!(
            "  {} ({} bytes)",
            entry.file_name().to_string_lossy(),
            meta.len()
        );
    }
}

#[test]
fn dump_fixed_array_comparison() {
    let cpp_dir = tempfile::tempdir().unwrap();
    let cpp_path = cpp_dir.path().join("cpp_table");
    casacore_test_support::cpp_table_write(CppTableFixture::FixedArray, &cpp_path)
        .expect("C++ write");

    // Dump table.f0
    let cpp_f0 = std::fs::read(cpp_path.join("table.f0")).expect("read cpp table.f0");
    eprintln!("=== C++ fixed_array table.f0 ({} bytes) ===", cpp_f0.len());
    hex_dump(&cpp_f0);

    // Also dump table.dat for column info
    let cpp_dat = std::fs::read(cpp_path.join("table.dat")).expect("read cpp table.dat");
    eprintln!(
        "\n=== C++ fixed_array table.dat ({} bytes) ===",
        cpp_dat.len()
    );
    hex_dump(&cpp_dat);

    // List all files
    eprintln!("\n=== C++ fixed_array files ===");
    for entry in std::fs::read_dir(&cpp_path).unwrap() {
        let entry = entry.unwrap();
        let meta = entry.metadata().unwrap();
        eprintln!(
            "  {} ({} bytes)",
            entry.file_name().to_string_lossy(),
            meta.len()
        );
    }
}

fn hex_dump(data: &[u8]) {
    for (i, chunk) in data.chunks(16).enumerate() {
        eprint!("{:04x}: ", i * 16);
        for (j, byte) in chunk.iter().enumerate() {
            eprint!("{:02x} ", byte);
            if j == 7 {
                eprint!(" ");
            }
        }
        // Pad if short
        for j in chunk.len()..16 {
            eprint!("   ");
            if j == 7 {
                eprint!(" ");
            }
        }
        eprint!(" |");
        for byte in chunk {
            if byte.is_ascii_graphic() || *byte == b' ' {
                eprint!("{}", *byte as char);
            } else {
                eprint!(".");
            }
        }
        eprintln!("|");
    }
}
