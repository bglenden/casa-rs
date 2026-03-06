// SPDX-License-Identifier: LGPL-3.0-or-later
//! Download and install the latest IERS EOP data.
//!
//! Usage: cargo run --example update_eop -p casacore-measures-data --features update -- [--data-dir DIR]
//!
//! If no directory is given, defaults to ~/.casa-rs/data/

fn main() {
    #[cfg(not(feature = "update"))]
    {
        eprintln!("This example requires the 'update' feature:");
        eprintln!("  cargo run --example update_eop -p casacore-measures-data --features update");
        std::process::exit(1);
    }

    #[cfg(feature = "update")]
    {
        let data_dir = parse_data_dir();
        println!("Downloading latest IERS EOP data...");
        println!("Destination: {}", data_dir.display());

        match casacore_measures_data::update::download_and_install(&data_dir) {
            Ok(casacore_measures_data::update::UpdateResult::Updated(path, summary)) => {
                println!("Updated: {}", path.display());
                println!("{summary}");
            }
            Ok(casacore_measures_data::update::UpdateResult::AlreadyCurrent(summary)) => {
                println!("Already up to date.");
                println!("{summary}");
            }
            Err(e) => {
                eprintln!("Error: {e}");
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
    // Default: ~/.casa-rs/data/
    dirs_or_home().join(".casa-rs").join("data")
}

#[cfg(feature = "update")]
fn dirs_or_home() -> std::path::PathBuf {
    std::env::var("HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("."))
}
