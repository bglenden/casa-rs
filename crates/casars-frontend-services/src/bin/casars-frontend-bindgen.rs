// SPDX-License-Identifier: LGPL-3.0-or-later

use std::env;
use std::process::ExitCode;

use camino::Utf8PathBuf;
use uniffi_bindgen::bindings::{PythonBindingGenerator, SwiftBindingGenerator};
use uniffi_bindgen::cargo_metadata::CrateConfigSupplier;
use uniffi_bindgen::library_mode::generate_bindings;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> anyhow::Result<()> {
    let mut args = env::args().skip(1);
    let language = args.next().ok_or_else(|| anyhow::anyhow!(usage()))?;
    let library_path = Utf8PathBuf::from(
        args.next()
            .ok_or_else(|| anyhow::anyhow!("missing library path\n{}", usage()))?,
    );
    let out_dir = Utf8PathBuf::from(
        args.next()
            .ok_or_else(|| anyhow::anyhow!("missing output directory\n{}", usage()))?,
    );
    let crate_name = Some("casars_frontend_services".to_string());
    let metadata = cargo_metadata::MetadataCommand::new().exec()?;
    let config_supplier = CrateConfigSupplier::from(metadata);

    match language.as_str() {
        "python" => {
            generate_bindings(
                &library_path,
                crate_name,
                &PythonBindingGenerator,
                &config_supplier,
                None,
                &out_dir,
                false,
            )?;
        }
        "swift" => {
            generate_bindings(
                &library_path,
                crate_name,
                &SwiftBindingGenerator,
                &config_supplier,
                None,
                &out_dir,
                false,
            )?;
        }
        other => anyhow::bail!("unsupported language {other:?}\n{}", usage()),
    }

    Ok(())
}

fn usage() -> &'static str {
    "usage: casars-frontend-bindgen <python|swift> <library-path> <out-dir>"
}
