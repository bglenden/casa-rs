// SPDX-License-Identifier: LGPL-3.0-or-later
//! casacore C++ reads the component lists casa-rs writes (simulation Phase 2,
//! #841).
//!
//! casa-rs writes the CASA fixture list. casacore's `ArrayMeasColumn<MDirection>`
//! then reads `Reference_Direction` through its `MEASINFO` (the frame from
//! `Direction_Frame`), and casacore's TaQL reads every scalar and array
//! column; both must agree with casa-rs. (casacore's TaQL does not print
//! record cells, so `Spectral_Record` is left to the CASA oracle,
//! `casa_oracle.rs`.)
//!
//! ```text
//! cargo test -p casa-components --features cpp-interop-tests --test casacore_cpp_interop
//! ```

#![cfg(feature = "cpp-interop-tests")]

mod common;

use casa_components::ComponentList;
use casa_tables::{Table, TableOptions};
use casa_test_support::casacore_oracle_available;
use casa_test_support::table_measures_interop::TableMeasuresOracle;
use casa_test_support::taql_interop::{TaqlOracle, results_match, rust_taql_query};
use casa_types::measures::direction::MDirection;
use common::{copy_table, fixture_dir};

const COLUMNS: &str = "Flux, Flux_Unit, Flux_Polarisation, Flux_Error, Shape, \
    Reference_Direction, Direction_Frame, Direction_Error, Direction_Error_Units, \
    Shape_Parameters, Shape_Error, Spectrum_Shape, Reference_Frequency, \
    Frequency_Frame, Frequency_Error, Frequency_Error_Units, Spectral_Parameters, \
    Spectral_Error, Label";

#[test]
fn casacore_reads_the_lists_casa_rs_writes() {
    if !casacore_oracle_available() {
        eprintln!("skipped: casacore C++ is not available (pkg-config casacore)");
        return;
    }
    let scratch = tempfile::tempdir().expect("scratch");
    let fixture = copy_table(&fixture_dir().join("components.cl"), scratch.path());
    let list = ComponentList::read_table(&fixture).expect("read fixture");
    let written = scratch.path().join("written.cl");
    list.write_table(&written).expect("write");
    let path = written.to_str().expect("UTF-8 path");

    let (values, frames) =
        TableMeasuresOracle::read_directions(path, "Reference_Direction", list.len())
            .expect("casacore reads Reference_Direction");
    for (row, component) in list.iter().enumerate() {
        let direction = component.shape().reference_direction();
        assert_eq!(frames[row], direction.refer().as_str(), "row {row}: frame");
        let casacore =
            MDirection::from_angles(values[2 * row], values[2 * row + 1], direction.refer());
        for (casacore, rust) in casacore.cosines().into_iter().zip(direction.cosines()) {
            assert!(
                (casacore - rust).abs() <= 4.0 * f64::EPSILON,
                "row {row}: {casacore} vs {rust}"
            );
        }
    }

    let cpp = TaqlOracle::query(&written, &format!("SELECT {COLUMNS} FROM $1"))
        .expect("casacore TaQL reads the list");
    let mut table = Table::open(TableOptions::new(&written)).expect("open");
    let rust = rust_taql_query(&mut table, &format!("SELECT {COLUMNS}")).expect("casa-rs TaQL");
    assert_eq!(cpp.nrow as usize, list.len());
    results_match(&rust, &cpp.grid, 0.0).expect("casacore and casa-rs read the same cells");
    println!(
        "casacore read {} rows x {} columns of the list casa-rs wrote, and its directions",
        cpp.nrow, cpp.ncol
    );
}
