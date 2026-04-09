// SPDX-License-Identifier: LGPL-3.0-or-later
//! Download and install the latest CASA-compatible measures runtime.
//!
//! Usage:
//! `cargo run --example update_measures -p casa-measures-data --features update -- [--data-dir DIR]`
//!
//! If no directory is given, defaults to `~/.casa/data`.

fn main() {
    #[cfg(not(feature = "update"))]
    {
        eprintln!("This example requires the 'update' feature:");
        eprintln!("  cargo run --example update_measures -p casa-measures-data --features update");
        std::process::exit(1);
    }

    #[cfg(feature = "update")]
    {
        let data_dir = parse_data_dir();
        println!(
            "Refreshing CASA-compatible measures runtime in {}",
            data_dir.display()
        );

        match casa_measures_data::update::refresh_measures_path(&data_dir) {
            Ok(result) => {
                println!("Installed: {}", result.path.display());
                println!("casarundata: {}", result.casarundata_version);
                println!("measures: {}", result.measures_version);
                println!("site: {}", result.measures_site);
            }
            Err(error) => {
                eprintln!("Error: {error}");
                std::process::exit(1);
            }
        }
    }
}

#[cfg(feature = "update")]
fn parse_data_dir() -> std::path::PathBuf {
    let args: Vec<String> = std::env::args().collect();
    for i in 1..args.len() {
        if args[i] == "--data-dir" {
            if let Some(dir) = args.get(i + 1) {
                return std::path::PathBuf::from(dir);
            }
        }
    }
    dirs_or_home().join(".casa").join("data")
}

#[cfg(feature = "update")]
fn dirs_or_home() -> std::path::PathBuf {
    std::env::var("HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("."))
}
