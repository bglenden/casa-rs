// SPDX-License-Identifier: LGPL-3.0-or-later

use super::run_ttable_like_demo;

#[test]
fn demo_contains_expected_section_headers() {
    let output = run_ttable_like_demo().expect("demo should run");
    assert!(output.contains("=== Table Demo"));
    assert!(output.contains("--- StManAipsIO round-trip"));
    assert!(output.contains("--- StandardStMan round-trip"));
    assert!(output.contains("--- StandardStMan-LE round-trip"));
    assert!(output.contains("--- IncrementalStMan round-trip"));
    assert!(output.contains("--- IncrementalStMan-LE round-trip"));
    assert!(output.contains("--- Column iteration patterns"));
    assert!(output.contains("--- Schema mutation"));
    assert!(output.contains("--- Reference tables"));
    assert!(output.contains("--- Sorting and table iteration"));
    assert!(output.contains("--- Table concatenation and copy"));
    assert!(output.contains("--- Column indexing"));
    #[cfg(unix)]
    assert!(output.contains("--- Table locking"));
    assert!(output.contains("--- Memory tables"));
    assert!(output.contains("--- TaQL (Table Query Language)"));
    assert!(output.contains("--- Tiled storage managers"));
    assert!(output.contains("--- Virtual column engines"));
    assert!(output.ends_with("end\n"));
}

#[test]
fn table_quantum_demo_runs() {
    let output = super::run_table_quantum_demo().expect("demo should succeed");
    assert!(output.contains("Fixed-unit scalar quantum column"));
    assert!(output.contains("Variable-unit scalar quantum column"));
    assert!(output.contains("Fixed-unit array quantum column"));
    assert!(output.contains("TableQuantum Demo complete"));
}
