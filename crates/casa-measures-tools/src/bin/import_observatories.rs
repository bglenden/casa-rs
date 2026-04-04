// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::PathBuf;

use casa_measures_tools::{
    import_observatories_table, observatories_table_candidates, write_observatories_snapshot,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut input = None::<PathBuf>;
    let mut output = None::<PathBuf>;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--input" => input = args.next().map(PathBuf::from),
            "--output" => output = args.next().map(PathBuf::from),
            "--help" | "-h" => {
                print_help();
                return Ok(());
            }
            value => return Err(format!("unknown argument {value:?}")),
        }
    }

    let input = input
        .or_else(|| {
            observatories_table_candidates()
                .into_iter()
                .find(|path| path.exists())
        })
        .ok_or_else(|| {
            "no observatories table found; pass --input /path/to/geodetic/Observatories".to_string()
        })?;
    let output = output.unwrap_or_else(default_output_path);

    let catalog = import_observatories_table(&input)?;
    write_observatories_snapshot(&catalog, &output)?;
    println!(
        "Wrote {} observatories from {} to {}",
        catalog.entries().len(),
        input.display(),
        output.display()
    );
    Ok(())
}

fn default_output_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate parent")
        .join("casa-measures-data")
        .join("data")
        .join("observatories.json")
}

fn print_help() {
    println!("Usage: import_observatories [--input PATH] [--output PATH]");
    println!();
    println!("Reads casacore geodetic/Observatories with the Rust table stack and");
    println!("writes the Rust-native observatory snapshot JSON used by");
    println!("casa-measures-data.");
}
