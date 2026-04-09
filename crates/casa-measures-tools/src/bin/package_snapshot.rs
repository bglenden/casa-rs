// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::PathBuf;

use casa_measures_tools::{create_packaged_snapshot, runtime_root_candidates};

fn main() {
    if let Err(error) = run() {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut input = None::<PathBuf>;
    let mut archive = None::<PathBuf>;
    let mut provenance = None::<PathBuf>;
    let mut args = std::env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--input" => input = args.next().map(PathBuf::from),
            "--archive" => archive = args.next().map(PathBuf::from),
            "--provenance" => provenance = args.next().map(PathBuf::from),
            "--help" | "-h" => {
                print_help();
                return Ok(());
            }
            value => return Err(format!("unknown argument {value:?}")),
        }
    }

    let input = input
        .or_else(|| {
            runtime_root_candidates()
                .into_iter()
                .find(|path| path.exists())
        })
        .ok_or_else(|| "no runtime root found; pass --input /path/to/.casa/data".to_string())?;
    let archive = archive.unwrap_or_else(default_archive_path);
    let provenance = provenance.unwrap_or_else(default_provenance_path);

    create_packaged_snapshot(&input, &archive, &provenance)?;
    println!("Runtime root: {}", input.display());
    println!("Archive: {}", archive.display());
    println!("Provenance: {}", provenance.display());
    Ok(())
}

fn default_archive_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate parent")
        .join("casa-measures-data")
        .join("data")
        .join("casa-measures-runtime.tar.gz")
}

fn default_provenance_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate parent")
        .join("casa-measures-data")
        .join("data")
        .join("casa-measures-runtime.provenance.json")
}

fn print_help() {
    println!("Usage: package_snapshot [--input PATH] [--archive PATH] [--provenance PATH]");
    println!();
    println!("Packages the CASA-table runtime subset used by casa-rs into the");
    println!("fallback archive plus provenance JSON stored under casa-measures-data/data/.");
}
