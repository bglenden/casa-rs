// SPDX-License-Identifier: LGPL-3.0-or-later
//! Internal maintenance helpers for generating bundled measures-data snapshots.

use std::path::{Path, PathBuf};

use casacore_measures_data::{ObservatoryCatalog, ObservatoryEntry};
use casacore_tables::{Table, TableOptions};
use casacore_types::ScalarValue;

/// Import casacore `geodetic/Observatories` into a Rust-native catalog.
pub fn import_observatories_table(input_path: &Path) -> Result<ObservatoryCatalog, String> {
    let table = Table::open(TableOptions::new(input_path)).map_err(|error| error.to_string())?;
    let mut entries = Vec::with_capacity(table.row_count());
    for row in 0..table.row_count() {
        entries.push(ObservatoryEntry {
            mjd: read_f64(&table, row, "MJD")?,
            name: read_string(&table, row, "Name")?,
            observatory_type: read_string(&table, row, "Type")?,
            longitude_deg: read_f64(&table, row, "Long")?,
            latitude_deg: read_f64(&table, row, "Lat")?,
            height_m: read_f64(&table, row, "Height")?,
            x_m: read_f64(&table, row, "X")?,
            y_m: read_f64(&table, row, "Y")?,
            z_m: read_f64(&table, row, "Z")?,
            source: read_string(&table, row, "Source")?,
            comment: read_string(&table, row, "Comment")?,
            antenna_responses: read_string(&table, row, "AntennaResponses")?,
        });
    }
    Ok(ObservatoryCatalog::from_entries(entries))
}

/// Write a JSON observatory snapshot to `output_path`.
pub fn write_observatories_snapshot(
    catalog: &ObservatoryCatalog,
    output_path: &Path,
) -> Result<(), String> {
    let content = catalog.to_json_pretty()?;
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    std::fs::write(output_path, content).map_err(|error| error.to_string())
}

/// Candidate installed casacore-data observatory table paths.
pub fn observatories_table_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    if let Ok(path) = std::env::var("CASA_RS_OBSERVATORIES_TABLE") {
        candidates.push(PathBuf::from(path));
    }

    candidates.extend(glob_candidates(
        "/opt/homebrew/Cellar/casacore-data/*/data/geodetic/Observatories",
    ));
    candidates.extend(glob_candidates(
        "/usr/local/Cellar/casacore-data/*/data/geodetic/Observatories",
    ));

    candidates
}

fn glob_candidates(pattern: &str) -> Vec<PathBuf> {
    let output = std::process::Command::new("sh")
        .arg("-c")
        .arg(format!("ls -d {pattern} 2>/dev/null"))
        .output();
    match output {
        Ok(output) if output.status.success() => String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(PathBuf::from)
            .collect(),
        _ => Vec::new(),
    }
}

fn read_string(table: &Table, row: usize, column: &str) -> Result<String, String> {
    match table
        .get_scalar_cell(row, column)
        .map_err(|error| error.to_string())?
    {
        ScalarValue::String(value) => Ok(value.clone()),
        value => Err(format!(
            "expected String in column {column} row {row}, found {value:?}"
        )),
    }
}

fn read_f64(table: &Table, row: usize, column: &str) -> Result<f64, String> {
    match table
        .get_scalar_cell(row, column)
        .map_err(|error| error.to_string())?
    {
        ScalarValue::Float64(value) => Ok(*value),
        ScalarValue::Float32(value) => Ok(f64::from(*value)),
        ScalarValue::Int32(value) => Ok(f64::from(*value)),
        ScalarValue::Int64(value) => Ok(*value as f64),
        value => Err(format!(
            "expected numeric scalar in column {column} row {row}, found {value:?}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{import_observatories_table, observatories_table_candidates};

    #[test]
    fn import_tool_reads_installed_observatories_when_available() {
        let Some(path) = observatories_table_candidates()
            .into_iter()
            .find(|path| path.exists())
        else {
            eprintln!(
                "skipping import_tool_reads_installed_observatories_when_available: no installed casacore-data Observatories table found"
            );
            return;
        };

        let catalog = import_observatories_table(&path).expect("import observatories");
        assert!(catalog.entries().len() > 40);
        assert!(catalog.get("ALMA").is_some());
        assert!(catalog.get("VLA").is_some());
        assert_eq!(
            catalog.entries().len(),
            casacore_measures_data::ObservatoryCatalog::bundled()
                .entries()
                .len()
        );
    }
}
