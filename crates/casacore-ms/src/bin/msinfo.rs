// SPDX-License-Identifier: LGPL-3.0-or-later
//! `msinfo` — summary information for a MeasurementSet.
//!
//! Equivalent to C++ `msoverview` / `listobs` summary output.
//!
//! # Usage
//!
//! ```text
//! msinfo <path-to-ms>
//! ```

use std::process;

use casacore_ms::ms::MeasurementSet;
use casacore_ms::selection_helpers;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: msinfo <path-to-ms>");
        process::exit(1);
    }

    let path = &args[1];
    match run(path) {
        Ok(()) => {}
        Err(e) => {
            eprintln!("Error: {e}");
            process::exit(1);
        }
    }
}

fn run(path: &str) -> Result<(), casacore_ms::MsError> {
    let ms = MeasurementSet::open(path)?;

    println!("MeasurementSet: {path}");
    println!("  Rows: {}", ms.row_count());

    if let Some(v) = ms.ms_version() {
        println!("  MS_VERSION: {v:.1}");
    }

    // Subtables
    let mut ids = ms.subtable_ids();
    ids.sort_by_key(|id| id.name());
    println!("  Subtables ({}):", ids.len());
    for id in &ids {
        if let Some(table) = ms.subtable(*id) {
            println!("    {:<25} {} rows", id.name(), table.row_count());
        }
    }

    // Antennas
    if let Ok(ant) = ms.antenna() {
        let n = ant.row_count();
        if n > 0 {
            println!("  Antennas ({n}):");
            for row in 0..n {
                let name = ant.name(row).unwrap_or_default();
                let station = ant.station(row).unwrap_or_default();
                let diam = ant.dish_diameter(row).unwrap_or(0.0);
                println!("    {row:3}: {name:<10} station={station:<10} diam={diam:.1}m");
            }
        }
    }

    // Fields
    if let Ok(field) = ms.field() {
        let n = field.row_count();
        if n > 0 {
            println!("  Fields ({n}):");
            for row in 0..n {
                let name = field.name(row).unwrap_or_default();
                let code = field.code(row).unwrap_or_default();
                println!("    {row:3}: {name:<16} code={code}");
            }
        }
    }

    // Spectral windows
    if let Ok(spw) = ms.spectral_window() {
        let n = spw.row_count();
        if n > 0 {
            println!("  Spectral Windows ({n}):");
            for row in 0..n {
                let nchan = spw.num_chan(row).unwrap_or(0);
                let ref_freq = spw.ref_frequency(row).unwrap_or(0.0);
                let bw = spw.total_bandwidth(row).unwrap_or(0.0);
                let name = spw.name(row).unwrap_or_default();
                println!(
                    "    {row:3}: {name:<12} nchan={nchan:4} ref_freq={:.3} MHz  bw={:.3} MHz",
                    ref_freq / 1e6,
                    bw / 1e6
                );
            }
        }
    }

    // Polarization
    if let Ok(pol) = ms.polarization() {
        let n = pol.row_count();
        if n > 0 {
            println!("  Polarization setups ({n}):");
            for row in 0..n {
                let ncorr = pol.num_corr(row).unwrap_or(0);
                let types = pol
                    .corr_type(row)
                    .map(|v| {
                        v.iter()
                            .map(|&c| stokes_name(c))
                            .collect::<Vec<_>>()
                            .join(" ")
                    })
                    .unwrap_or_default();
                println!("    {row:3}: ncorr={ncorr}  types=[{types}]");
            }
        }
    }

    // Observation
    if let Some(obs_table) = ms.subtable(casacore_ms::SubtableId::Observation) {
        let n = obs_table.row_count();
        if n > 0 {
            use casacore_types::ScalarValue;
            println!("  Observations ({n}):");
            for row in 0..n {
                let telescope = obs_table
                    .get_scalar_cell(row, "TELESCOPE_NAME")
                    .ok()
                    .and_then(|v| match v {
                        ScalarValue::String(s) => Some(s.clone()),
                        _ => None,
                    })
                    .unwrap_or_default();
                let observer = obs_table
                    .get_scalar_cell(row, "OBSERVER")
                    .ok()
                    .and_then(|v| match v {
                        ScalarValue::String(s) => Some(s.clone()),
                        _ => None,
                    })
                    .unwrap_or_default();
                let project = obs_table
                    .get_scalar_cell(row, "PROJECT")
                    .ok()
                    .and_then(|v| match v {
                        ScalarValue::String(s) => Some(s.clone()),
                        _ => None,
                    })
                    .unwrap_or_default();
                println!(
                    "    {row:3}: telescope={telescope}  observer={observer}  project={project}"
                );
            }
        }
    }

    // Scan summary
    {
        use casacore_types::ScalarValue;
        let table = ms.main_table();
        let mut scans = std::collections::BTreeMap::<i32, usize>::new();
        for row in 0..table.row_count() {
            if let Ok(ScalarValue::Int32(scan)) = table.get_scalar_cell(row, "SCAN_NUMBER") {
                *scans.entry(*scan).or_insert(0) += 1;
            }
        }
        if !scans.is_empty() {
            println!("  Scans ({}):", scans.len());
            for (scan, count) in &scans {
                println!("    scan {scan:4}: {count} rows");
            }
        }
    }

    // Time range
    if let Ok(Some((tmin, tmax))) = selection_helpers::time_range(&ms) {
        println!("  Time range: {tmin:.6} — {tmax:.6} (MJD seconds)");
    }

    // Data column shapes
    {
        let table = ms.main_table();
        for col_name in &["DATA", "CORRECTED_DATA", "MODEL_DATA", "FLOAT_DATA"] {
            if let Some(schema) = table.schema() {
                if schema.contains_column(col_name) {
                    if table.row_count() > 0 {
                        if let Ok(arr) = table.get_array_cell(0, col_name) {
                            println!("  {col_name}: shape {:?}", arr.shape());
                        }
                    } else {
                        println!("  {col_name}: present (no rows)");
                    }
                }
            }
        }
    }

    Ok(())
}

/// Map Stokes enum value to name (subset of casacore Stokes.h).
fn stokes_name(code: i32) -> &'static str {
    match code {
        1 => "I",
        2 => "Q",
        3 => "U",
        4 => "V",
        5 => "RR",
        6 => "RL",
        7 => "LR",
        8 => "LL",
        9 => "XX",
        10 => "XY",
        11 => "YX",
        12 => "YY",
        _ => "??",
    }
}
