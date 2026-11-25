// SPDX-License-Identifier: LGPL-3.0-or-later
//! Table keywords that name other tables, written by casa-rs and resolved by
//! C++ casacore after the table tree has moved (#715).
//!
//! casa-rs stores an absolute table keyword relative to the table that holds
//! it, as casacore's `TableRecordRep::putData` does: `././<rest>` inside the
//! table, `./<rest>` under its directory, `<rest>/.` for an enclosing table.
//!
//! These tests are skipped when `pkg-config casacore` is not available.

#![cfg(feature = "cpp-interop-tests")]

use std::path::{Path, PathBuf};

use casa_tables::{ColumnSchema, Table, TableOptions, TableSchema};
use casa_test_support::{TableOracle, casacore_oracle_available};
use casa_types::{PrimitiveType, RecordField, RecordValue, Value};

fn table_ref(target: &Path) -> Value {
    Value::table_ref(target.to_string_lossy())
}

/// Saves a one-column table at `path` with absolute table keywords
/// `keywords` and, on its column `TIME`, `column_keywords`.
fn save_table(path: &Path, keywords: &[(&str, &Path)], column_keywords: &[(&str, &Path)]) {
    let schema = TableSchema::new(vec![ColumnSchema::scalar("TIME", PrimitiveType::Float64)])
        .expect("valid schema");
    let mut table = Table::with_schema(schema);
    for (name, target) in keywords {
        table.keywords_mut().upsert(*name, table_ref(target));
    }
    table.set_column_keywords(
        "TIME",
        RecordValue::new(
            column_keywords
                .iter()
                .map(|(name, target)| RecordField::new(*name, table_ref(target)))
                .collect(),
        ),
    );
    table.save(TableOptions::new(path)).expect("save table");
}

fn canonical(path: impl AsRef<Path>) -> PathBuf {
    path.as_ref().canonicalize().expect("existing path")
}

/// RC after a move: casacore opens every table keyword casa-rs wrote.
#[test]
fn rc_absolute_table_keywords_resolve_in_casacore_after_a_move() {
    if !casacore_oracle_available() {
        eprintln!("skipping RC table keyword test: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("create temp dir");
    let root = dir.path().join("run");
    let main = root.join("main.tbl");
    let sub = main.join("SUB");
    let cal = root.join("cal").join("cal.tbl");
    std::fs::create_dir_all(cal.parent().unwrap()).unwrap();
    save_table(&main, &[("SUB", &sub), ("CAL", &cal)], &[("CAL", &cal)]);
    save_table(&sub, &[("PARENT", &main)], &[]);
    save_table(&cal, &[], &[]);

    let moved = dir.path().join("moved").join("deeper").join("run");
    std::fs::create_dir_all(moved.parent().unwrap()).unwrap();
    std::fs::rename(&root, &moved).unwrap();
    let main = moved.join("main.tbl");
    let resolved = |table: &Path, column: Option<&str>, keyword: &str| {
        canonical(
            TableOracle::keyword_table_name(table, column, keyword)
                .unwrap_or_else(|error| panic!("C++ opens {keyword}: {error}")),
        )
    };
    assert_eq!(resolved(&main, None, "SUB"), canonical(main.join("SUB")));
    assert_eq!(
        resolved(&main, None, "CAL"),
        canonical(moved.join("cal").join("cal.tbl"))
    );
    assert_eq!(
        resolved(&main, Some("TIME"), "CAL"),
        canonical(moved.join("cal").join("cal.tbl"))
    );
    assert_eq!(
        resolved(&main.join("SUB"), None, "PARENT"),
        canonical(&main)
    );
}
